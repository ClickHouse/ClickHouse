//! Exhaustive small-domain checks of the descriptor and write arithmetic, each paired with
//! hand-written mutants that the same checker must reject (so the checker is not vacuous).
#![allow(unsafe_code)]

use _ch_rust_dwarf::error::{DwarfError, Result};
use _ch_rust_dwarf::outbuf::*;
use core::mem::MaybeUninit;

// ---- checked_required_cap ------------------------------------------------------------

fn check_required_cap(
    f: impl Fn(usize, usize, usize) -> Result<Option<usize>>,
) -> core::result::Result<(), String> {
    let max_cap = if cfg!(miri) { 8usize } else { 64usize };
    let max_n = if cfg!(miri) { 10usize } else { 70usize };
    for cap in 0..=max_cap {
        for len in 0..=max_n {
            for add in 0..=max_n {
                let got = f(len, cap, add);
                if len > cap {
                    if got.is_ok() {
                        return Err(format!("len {len} > cap {cap} accepted"));
                    }
                    continue;
                }
                match got {
                    Err(_) => return Err(format!("({len},{cap},{add}) rejected")),
                    Ok(None) if len + add > cap => {
                        return Err(format!("({len},{cap},{add}) claims to fit"));
                    }
                    Ok(Some(n)) if n != len + add || n <= cap => {
                        return Err(format!("({len},{cap},{add}) -> {n}"));
                    }
                    _ => {}
                }
            }
        }
    }
    if f(usize::MAX - 3, usize::MAX, 4).is_ok() || f(1, usize::MAX, usize::MAX).is_ok() {
        return Err("overflow handling".into());
    }
    if !matches!(f(0, usize::MAX, usize::MAX), Ok(None))
        || !matches!(f(usize::MAX, usize::MAX, 0), Ok(None))
    {
        return Err("boundary at usize::MAX".into());
    }
    Ok(())
}

#[test]
fn required_cap_invariants_hold_exhaustively() {
    check_required_cap(checked_required_cap).unwrap();
}

#[test]
fn required_cap_mutants_are_caught() {
    assert!(check_required_cap(|len, cap, add| {
        if len > cap {
            return Err(DwarfError::invalid_arg("x"));
        }
        let needed = len + add;
        Ok(if needed < cap { None } else { Some(needed) })
    })
    .is_err());
    assert!(check_required_cap(|len, cap, add| {
        let needed = len + add;
        Ok(if needed <= cap { None } else { Some(needed) })
    })
    .is_err());
    assert!(check_required_cap(|len, cap, add| {
        if len > cap {
            return Err(DwarfError::invalid_arg("x"));
        }
        let needed = len.wrapping_add(add);
        Ok(if needed <= cap { None } else { Some(needed) })
    })
    .is_err());
    assert!(check_required_cap(|len, cap, add| {
        if len > cap {
            return Err(DwarfError::invalid_arg("x"));
        }
        Ok(if len + add <= cap {
            None
        } else {
            Some(cap + add)
        })
    })
    .is_err());
}

// ---- element writers -----------------------------------------------------------------

type Writer = fn(&mut [MaybeUninit<u8>], usize, u64) -> Result<()>;

fn w16(b: &mut [MaybeUninit<u8>], i: usize, v: u64) -> Result<()> {
    write_u16_ne(b, i, v as u16)
}
fn w32(b: &mut [MaybeUninit<u8>], i: usize, v: u64) -> Result<()> {
    write_u32_ne(b, i, v as u32)
}

fn snapshot(buf: &[MaybeUninit<u8>]) -> Vec<u8> {
    // SAFETY: every slot of the test buffers is initialized by `MaybeUninit::new`.
    buf.iter().map(|s| unsafe { s.assume_init() }).collect()
}

fn check_writer(width: usize, write: Writer) -> core::result::Result<(), String> {
    let val = 0x0807_0605_0403_0201u64;
    for len in 0..=40usize {
        for idx in 0..=20usize {
            let mut buf = vec![MaybeUninit::new(0xAAu8); len];
            let fits = (idx + 1) * width <= len;
            let r = write(&mut buf, idx, val);
            let after = snapshot(&buf);
            if fits != r.is_ok() {
                return Err(format!("len {len} idx {idx}: ok={}", r.is_ok()));
            }
            let mut want = vec![0xAAu8; len];
            if fits {
                let bytes = val.to_ne_bytes();
                let src = if cfg!(target_endian = "little") {
                    &bytes[..width]
                } else {
                    &bytes[8 - width..]
                };
                want[idx * width..(idx + 1) * width].copy_from_slice(src);
            }
            if after != want {
                return Err(format!("len {len} idx {idx}: wrong bytes {after:?}"));
            }
        }
    }
    Ok(())
}

#[test]
fn element_writers_are_exact_and_bounds_checked() {
    check_writer(2, w16).unwrap();
    check_writer(4, w32).unwrap();
    check_writer(8, write_u64_ne).unwrap();
}

#[test]
fn writer_mutants_are_caught() {
    assert!(check_writer(8, |b, i, v| write_u64_ne(b, i.saturating_sub(1), v)).is_err());
    assert!(check_writer(4, |b, i, v| {
        if (i + 1) * 4 > b.len() + 4 {
            return Err(DwarfError::internal("x"));
        }
        write_u32_ne(b, i, v as u32).or(Ok(()))
    })
    .is_err());
    assert!(check_writer(2, |b, i, v| write_u16_ne(b, i, (v as u16) >> 8)).is_err());
}

#[test]
fn byte_writer_is_exact() {
    for len in 0..=24usize {
        for start in 0..=26usize {
            for n in 0..=10usize {
                let payload: Vec<u8> = (1..=n as u8).collect();
                let mut buf = vec![MaybeUninit::new(0xAAu8); len];
                let r = write_bytes(&mut buf, start, &payload);
                let after = snapshot(&buf);
                let mut want = vec![0xAAu8; len];
                let fits = start + n <= len;
                if fits {
                    want[start..start + n].copy_from_slice(&payload);
                }
                assert_eq!(r.is_ok(), fits, "len {len} start {start} n {n}");
                assert_eq!(after, want);
            }
        }
    }
    let mut buf = vec![MaybeUninit::new(0u8); 4];
    assert!(write_bytes(&mut buf, usize::MAX, &[1, 2]).is_err());
}

// ---- projection plan -----------------------------------------------------------------

fn expected_active(mask: u32, id: SinkId) -> bool {
    let bit = |n: u32| mask & (1 << n) != 0;
    match id {
        SinkId::Offset => bit(0),
        SinkId::Size => bit(1),
        SinkId::Tag => bit(2),
        SinkId::AncestorTags => bit(5),
        SinkId::AncestorOffsets => bit(6),
        SinkId::AncestorArrayOffsets => bit(5) || bit(6),
        SinkId::NameChars | SinkId::NameOffsets => bit(7),
        SinkId::LinkageNameChars | SinkId::LinkageNameOffsets => bit(8),
        SinkId::DeclFileChars | SinkId::DeclFileOffsets => bit(9),
        SinkId::DeclLine => bit(10),
        SinkId::RangesStart | SinkId::RangesEnd | SinkId::RangesOffsets => bit(11),
        SinkId::AttrName => bit(12),
        SinkId::AttrForm => bit(13),
        SinkId::AttrInt => bit(14),
        SinkId::AttrStrChars | SinkId::AttrStrOffsets => bit(15),
        SinkId::AttrOffsets => bit(12) || bit(13) || bit(14) || bit(15),
    }
}

const ALL_IDS: [SinkId; SINK_COUNT] = [
    SinkId::Offset,
    SinkId::Size,
    SinkId::Tag,
    SinkId::AncestorTags,
    SinkId::AncestorOffsets,
    SinkId::AncestorArrayOffsets,
    SinkId::NameChars,
    SinkId::NameOffsets,
    SinkId::LinkageNameChars,
    SinkId::LinkageNameOffsets,
    SinkId::DeclFileChars,
    SinkId::DeclFileOffsets,
    SinkId::DeclLine,
    SinkId::RangesStart,
    SinkId::RangesEnd,
    SinkId::RangesOffsets,
    SinkId::AttrName,
    SinkId::AttrForm,
    SinkId::AttrInt,
    SinkId::AttrStrChars,
    SinkId::AttrStrOffsets,
    SinkId::AttrOffsets,
];

fn check_plan(active: impl Fn(ProjectionPlan, SinkId) -> bool) -> core::result::Result<(), String> {
    let masks: Box<dyn Iterator<Item = u32>> = if cfg!(miri) {
        Box::new(
            [0u32, 0xffff, (1 << 5) | (1 << 6), 0xf << 12]
                .into_iter()
                .chain((0..18).map(|b| 1u32 << b)),
        )
    } else {
        Box::new(0..(1u32 << 17))
    };
    for mask in masks {
        for anc in 0..3u32 {
            for attr in 0..3u32 {
                for reserved in 0..2u32 {
                    let want_anc = mask & ((1 << 5) | (1 << 6)) != 0;
                    let want_attr = mask & (0xf << 12) != 0;
                    let valid = mask < (1 << 16)
                        && reserved == 0
                        && anc == u32::from(want_anc)
                        && attr == u32::from(want_attr);
                    let got = ProjectionPlan::validate_and_build(mask, anc, attr, reserved);
                    if got.is_ok() != valid {
                        return Err(format!(
                            "mask {mask:#x} anc {anc} attr {attr} reserved {reserved}"
                        ));
                    }
                    if let Ok(plan) = got {
                        for id in ALL_IDS {
                            if active(plan, id) != expected_active(mask, id) {
                                return Err(format!("mask {mask:#x}: sink {id:?}"));
                            }
                        }
                        if plan.mask != mask
                            || plan.need_ancestor_array_offsets != want_anc
                            || plan.need_attr_offsets != want_attr
                        {
                            return Err(format!("mask {mask:#x}: plan fields"));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn projection_plans_are_exhaustively_consistent() {
    check_plan(|p, id| p.sink_active(id)).unwrap();
    for id in ALL_IDS {
        assert!((id as usize) < SINK_COUNT);
        assert!(matches!(id.expected_elem_size(), 1 | 2 | 4 | 8));
    }
}

#[test]
fn projection_plan_mutants_are_caught() {
    assert!(check_plan(|p, id| match id {
        SinkId::AncestorArrayOffsets => p.has(COL_ANCESTOR_TAGS),
        _ => p.sink_active(id),
    })
    .is_err());
    assert!(check_plan(|p, id| match id {
        SinkId::AttrOffsets => p.has(COL_ATTR_NAME),
        _ => p.sink_active(id),
    })
    .is_err());
    assert!(check_plan(|p, id| match id {
        SinkId::DeclFileOffsets => false,
        _ => p.sink_active(id),
    })
    .is_err());
}
