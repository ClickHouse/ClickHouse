//! The executor: runs a [`Program`] over a block of Avro binary data, writing
//! into caller-owned buffers.
//!
//! * **Resumable, row-transactional.** [`decode_partial`] returns when
//!   `max_rows` rows are done ([`Status::RowLimit`]), when the input ended
//!   exactly at a row boundary ([`Status::BlockDone`]), or when a sink is full
//!   ([`Status::NeedSpace`]). A row that fails is rolled back (the sinks' fill
//!   levels are restored to the end of the last completed row) and
//!   `rows_done`/`consumed` cover completed rows only.
//! * **`NeedSpace` pauses, it does not restart.** When the first row of a call
//!   needs more room the executor keeps the row's state (instruction, loops,
//!   input position, partial sink contents) and the caller calls again with
//!   the same input and state after growing the reported sinks ([`State::needs`]:
//!   every sink that lacks room for the block, so a record of many fields is one
//!   stop): the row continues where it stopped, so the work of a row does not depend on how
//!   many sinks grow while it is decoded. When a later row of the call needs
//!   room, the completed rows are reported and that row is rolled back and
//!   becomes the first row of the next call.
//! * **`NeedInput` pauses too, and drops what it is done with.** In [streaming mode](State::set_streaming) the
//!   input ending inside a row is not an error but a pause of the first row of the call, like `NeedSpace`, except
//!   that the call reports how much of the input the row is done with (`consumed`) and the next call is given the
//!   input from there on, extended with the bytes that arrived. A skipped `bytes`, `string` or `fixed` is consumed
//!   as it arrives, so the caller never holds a row, however large the values that it does not read are.
//! * **Non-recursive.** Loops (arrays, maps) and calls (shared or recursive
//!   named records that are skipped) live on an explicit frame stack bounded by
//!   [`State::new`]'s `max_depth`.
//! * **Allocation free.** Besides that stack, a fill-level snapshot of one
//!   entry per sink and the list of the sinks that lack room (at most one entry per sink), the executor allocates nothing.
//! * **Bounded work.** A block count is compared with the bytes that remain
//!   (`count <= remaining / min_item_size`) before any item is decoded.
//!   Items that occupy no bytes are capped per block by
//!   [`crate::MAX_ZERO_SIZE_ITEMS`]. Every iteration of every loop either
//!   consumes input or spends one of those bounded counts.
//! * **Errors** carry the committed progress ([`DecodeError`]); the state is
//!   reset, so the same state can be used for the next block. The exception is
//!   an input that ends inside a row ([`ErrorCode::Eof`],
//!   [`ErrorCode::CountExceedsPayload`]): the row is decoded again from its
//!   start once more bytes arrive, so the block's budget of zero-size items,
//!   counted over the rows that completed, is kept.
//!
//! All buffers are byte slices, so no alignment is required from the caller.
//! Numbers are written in host byte order, which is what a ClickHouse column
//! stores. Avro `float` and `double` are read as little-endian, as the
//! specification says.

use std::mem::MaybeUninit;

use crate::outbuf::OutBuf;
use crate::program::{Op, Program, UnionMode, NO_CHECK, NO_RESERVE, NO_SINK};
use crate::varint::{decode_int, decode_long, varint_len, VarintError};
use crate::{Error, ErrorCode, Result, HARD_MAX_DEPTH, MAX_ZERO_SIZE_ITEMS};

/// A caller-owned output buffer pair and its fill levels.
///
/// The buffers are [`OutBuf`]s: their bytes past the fill levels may be uninitialized (a `resize` of a
/// ClickHouse `PODArray` leaves them so), and the executor only ever writes to them, so it needs no
/// initialized bytes from the caller.
///
/// For fixed-width kinds only `data` is used and `len` counts elements. For
/// [`SinkKind::Bytes`], `data` holds the value bytes (`len` bytes used) and
/// `data2` the `u64` end offsets (`len2` used). `offset` is the running end
/// offset for `Bytes` and the offsets sinks: the next offset written is
/// `offset` plus the size of the value; set it to the last existing offset of
/// the column before the first call.
pub struct Sink<'a> {
    pub data: OutBuf<'a>,
    pub data2: OutBuf<'a>,
    pub len: usize,
    pub len2: usize,
    pub offset: u64,
}

impl<'a> Sink<'a> {
    /// A sink over initialized bytes.
    pub fn new(data: &'a mut [u8]) -> Sink<'a> {
        Sink::from_bufs(data.into(), OutBuf::empty())
    }

    /// A sink with offsets over initialized bytes.
    pub fn with_offsets(data: &'a mut [u8], offsets: &'a mut [u8]) -> Sink<'a> {
        Sink::from_bufs(data.into(), offsets.into())
    }

    /// A sink over bytes that may be uninitialized.
    pub fn new_uninit(data: &'a mut [MaybeUninit<u8>]) -> Sink<'a> {
        Sink::from_bufs(data.into(), OutBuf::empty())
    }

    /// A sink with offsets over bytes that may be uninitialized.
    pub fn with_offsets_uninit(
        data: &'a mut [MaybeUninit<u8>],
        offsets: &'a mut [MaybeUninit<u8>],
    ) -> Sink<'a> {
        Sink::from_bufs(data.into(), offsets.into())
    }

    /// A sink over `data` and `data2`, empty.
    pub fn from_bufs(data: OutBuf<'a>, data2: OutBuf<'a>) -> Sink<'a> {
        Sink {
            data,
            data2,
            len: 0,
            len2: 0,
            offset: 0,
        }
    }
}

/// A sink that has no room for what a block (or one value) needs, and what it must hold counted from its fill
/// level: elements of the primary buffer and of the secondary one. `repr(C)`: the C ABI hands the list of them
/// to the caller as an array of `AvroNeed`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Need {
    pub sink: u32,
    pub min_cap: u64,
    pub min_cap2: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// The input ended exactly at a row boundary. Only reported when rows
    /// cannot occupy zero bytes (`Program::min_row_size() > 0`).
    BlockDone,
    /// A sink is full. Grow the primary buffer to hold at least `min_cap`
    /// elements (bytes for `Bytes`) and the secondary one to at least
    /// `min_cap2` elements, then call again.
    ///
    /// `min_cap` is the room for the next value, or for more: a block of 16 or more array or map items
    /// that the bytes of the input can hold asks, before the first of them is read, for the room of
    /// all of them in each sink that every item writes to (see [`crate::program::Program`]'s loops: the
    /// start of the body up to its first union), so that a caller does one growth per sink instead of
    /// one for every doubling.
    ///
    /// The sink and the capacities here are the first entry of [`State::needs`]. That list has every sink
    /// of the block that lacks room, and a caller that grows all of them costs one stop for the block
    /// instead of one for each sink. A caller that grows only the first is told about the others by the
    /// stops that follow.
    NeedSpace {
        sink: u32,
        min_cap: u64,
        min_cap2: u64,
    },
    /// `max_rows` rows were decoded. Also the end of a row that was paused for input and has been completed:
    /// such a call stops after that row, whatever `max_rows` is.
    RowLimit,
    /// Only in [streaming mode](State::set_streaming): the input ended inside a row, and more bytes may follow.
    /// `consumed` bytes of the input (and `rows_done` rows) are done with; the call that follows is given the
    /// input from `consumed` on, extended with the bytes that arrived since.
    ///
    /// With `rows_done == 0` the row is *paused* where it stopped: the sinks keep what it wrote, and the state
    /// its instruction, loops, zero-size item budget and, for a `bytes`, `string` or `fixed` that is skipped
    /// and was cut, how many of its bytes are still to skip. The row continues with the same state and the
    /// same sinks, and `consumed` covers every byte of the row that the executor is done with, so the
    /// caller needs to keep only the bytes from `consumed` on. With `rows_done > 0` the row that was cut is
    /// discarded as for [`Status::NeedSpace`] and its bytes are decoded again by the next call, which pauses it.
    NeedInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Progress {
    /// Rows completed by this call.
    pub rows_done: u64,
    /// Bytes of the input consumed by this call.
    pub consumed: usize,
    pub status: Status,
}

enum ExecFrame {
    Loop { remaining: u64, total: u64 },
    Call { ret: u32 },
}

/// Deterministic work counters of a [`State`], summed over its lifetime.
///
/// They let tests (and callers that want to know) check that the work of a decode is bounded by the
/// payload and the output instead of timing it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Calls of [`decode_partial`].
    pub calls: u64,
    /// Calls that returned [`Status::NeedSpace`].
    pub need_space: u64,
    /// Calls that returned [`Status::NeedInput`].
    pub need_input: u64,
    /// Times a row that had been decoded in part was discarded and will be decoded again from its start.
    /// A `NeedSpace` or `NeedInput` in the first row of a call only pauses it and is not counted here.
    pub restarts: u64,
    /// Input bytes that were decoded a second time: the bytes of the partial rows that were discarded and
    /// of the completed rows that a rollback replays.
    pub replayed_bytes: u64,
    /// Sinks whose buffers and fill levels a call validated before running. A call that starts a row
    /// validates all of them; a call that resumes a paused row validates the sinks that [`State::needs`]
    /// named, or none if the row was paused for input.
    pub sinks_checked: u64,
    /// Entries of the reservation lists of loops that were looked at to ask for the room of a block (see
    /// [`Status::NeedSpace`]). A block that needs room in `n` sinks costs `n` of them for the stop that asks
    /// and `n` for the look that finds the room there: not `n * (n + 1) / 2`.
    pub reserve_visits: u64,
    /// Array and map items that a loop whose body is one number decoded in a run, without going back to
    /// the dispatch of the executor for each of them.
    pub bulk_items: u64,
}

pub struct State {
    ip: u32,
    frames: Vec<ExecFrame>,
    /// Zero-size items accepted so far in the current block.
    zero_items: u64,
    max_depth: usize,
    /// Sink fill levels at the first row boundary of the current call, for rollback. A call that resumes
    /// a paused row keeps the levels of the call that started the row: they are the row's start.
    fill: Vec<(usize, usize, u64)>,
    /// `zero_items` at that same row boundary.
    zero_items_start: u64,
    /// Rows completed so far by the current call.
    rows_call: u64,
    /// `Some(pos)`: the last call stopped with `NeedSpace` or `NeedInput` in its first row, and the row
    /// continues at byte `pos` of the input of the next call: where it stopped, for `NeedSpace`, which is
    /// resumed with the same bytes; `0` for `NeedInput`, whose input starts at what was not consumed.
    /// `ip`, `frames` and `zero_items` are the row's executor state; the sinks hold what it wrote.
    paused: Option<usize>,
    /// The paused row began in input that the caller has dropped (`NeedInput`), so it cannot be replayed
    /// from the input of a call: the call that continues it stops when it is done.
    carried: bool,
    /// Bytes of a skipped `bytes`, `string` or `fixed` that a paused row still has to skip in the
    /// following input; `ip` is then the instruction after it.
    skip_left: u64,
    /// The sinks that lack room for the block or value that the last [`Status::NeedSpace`] stopped at.
    needs: Vec<Need>,
    /// The input may continue after the bytes of a call; see [`State::set_streaming`].
    streaming: bool,
    stats: Stats,
}

impl State {
    /// `max_depth` bounds nested arrays, maps and calls at once; `0` selects
    /// the internal hard cap.
    pub fn new(max_depth: usize) -> State {
        State {
            ip: 0,
            frames: Vec::new(),
            zero_items: 0,
            fill: Vec::new(),
            rows_call: 0,
            zero_items_start: 0,
            paused: None,
            carried: false,
            skip_left: 0,
            needs: Vec::new(),
            streaming: false,
            stats: Stats::default(),
            max_depth: if max_depth == 0 {
                HARD_MAX_DEPTH
            } else {
                max_depth.min(HARD_MAX_DEPTH)
            },
        }
    }

    fn reset_row(&mut self) {
        self.ip = 0;
        self.frames.clear();
        self.paused = None;
        self.carried = false;
        self.skip_left = 0;
    }

    /// Back to the start of a row for a new block, or after an error.
    pub fn reset(&mut self) {
        self.reset_row();
        self.zero_items = 0;
        self.needs.clear();
    }

    /// Streaming mode: the caller has more bytes of the block than the input it passes. When the input ends
    /// inside a row the call reports [`Status::NeedInput`] instead of the error [`ErrorCode::Eof`] (or
    /// [`ErrorCode::CountExceedsPayload`]) and keeps the row, so that the caller drops the bytes that the
    /// row is done with and does not hold a whole row, however large the values of it that are skipped.
    ///
    /// Off by default, and a mode of the caller, not of the block: it may be changed between calls, also
    /// while a row is paused. A caller that has no more bytes switches it off and calls again with what it
    /// has: the paused row then fails with the error that the end of the input is.
    pub fn set_streaming(&mut self, streaming: bool) {
        self.streaming = streaming;
    }

    /// The work counters of this state since it was created. [`State::reset`] does not clear them.
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// Every sink that lacked room for the block or value at which the last [`Status::NeedSpace`] stopped,
    /// with what it must hold, counted from its fill level: the first entry is the sink and capacities
    /// that the status names. A caller that grows all of them before it calls again is not stopped for the
    /// block a second time. Valid until the next call.
    pub fn needs(&self) -> &[Need] {
        &self.needs
    }

    /// True between rows: nothing is half decoded and no row is paused for space or input.
    pub fn at_row_boundary(&self) -> bool {
        self.ip == 0 && self.frames.is_empty() && self.paused.is_none()
    }
}

fn eof() -> Error {
    Error::new(ErrorCode::Eof, "EOF reached")
}

fn too_deep() -> Error {
    Error::new(
        ErrorCode::DataTooDeep,
        "Avro data nesting depth exceeds the limit",
    )
}

fn check_sink_count(program: &Program, sinks: &[Sink<'_>]) -> Result<()> {
    if sinks.len() != program.sinks.len() {
        return Err(Error::new(
            ErrorCode::InvalidArgument,
            format!(
                "expected {} sinks, got {}",
                program.sinks.len(),
                sinks.len()
            ),
        ));
    }
    Ok(())
}

/// `sinks[i]` holds its fill levels in its buffers. `sinks` has as many entries as `program`.
fn check_sink(program: &Program, sinks: &[Sink<'_>], i: usize) -> Result<()> {
    let (s, d) = (&sinks[i], &program.sinks[i]);
    let ok = s
        .len
        .checked_mul(d.width())
        .is_some_and(|b| b <= s.data.len())
        && s.len2
            .checked_mul(d.width2())
            .is_some_and(|b| b <= s.data2.len())
        && (d.width2() != 0 || s.len2 == 0);
    if ok {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::InvalidArgument,
            format!("sink {i}: fill level exceeds the buffer"),
        ))
    }
}

fn check_sinks(program: &Program, sinks: &[Sink<'_>]) -> Result<()> {
    check_sink_count(program, sinks)?;
    (0..sinks.len()).try_for_each(|i| check_sink(program, sinks, i))
}

/// Reads an array or map block header at `pos`. Returns the item count (0
/// ends the array) and the position after the header. A negative count means
/// `-count` items preceded by a byte size, which is read and ignored.
fn read_block_count(input: &[u8], pos: usize) -> Result<(u64, usize)> {
    let (c, n) = decode_long(&input[pos..]).map_err(VarintError::into_error)?;
    let mut p = pos + n;
    if c >= 0 {
        return Ok((c as u64, p));
    }
    let Some(count) = c.checked_neg() else {
        return Err(Error::new(
            ErrorCode::InvalidCount,
            format!("Invalid array or map block count: {c}"),
        ));
    };
    let (_size, n) = decode_long(&input[p..]).map_err(VarintError::into_error)?;
    p += n;
    Ok((count as u64, p))
}

/// Rejects a block of `count` items that cannot be real.
fn check_block(count: u64, item_min: u64, remaining: usize, zero_items: &mut u64) -> Result<()> {
    if item_min == 0 {
        let total = zero_items.saturating_add(count);
        if count > MAX_ZERO_SIZE_ITEMS || total > MAX_ZERO_SIZE_ITEMS {
            return Err(Error::new(
                ErrorCode::TooManyItems,
                format!("Avro array or map of {count} items that occupy no bytes exceeds the limit of {MAX_ZERO_SIZE_ITEMS} per block"),
            ));
        }
        *zero_items = total;
    } else if count > (remaining as u64) / item_min {
        return Err(Error::new(
            ErrorCode::CountExceedsPayload,
            format!(
                "Avro array or map block count {count} cannot fit in {remaining} remaining byte(s)"
            ),
        ));
    }
    Ok(())
}

/// Blocks of fewer items than this are left to the room checks of the items: asking costs a pass over what the
/// loop writes, which a small block does not repay.
const RESERVE_MIN_ITEMS: u64 = 16;

/// Whether a sink that the loop with the list at `reserve` writes to has no room for a block of `count` items.
/// Then `needs` holds every one of them (and only them), each as what it must hold counted from its fill
/// level, so that the caller grows them all in one stop. Without a need `needs` is left as it is: `false` if
/// every sink has the room, or the block is too small to ask for it.
///
/// The caller has checked that the bytes of the input can hold `count` items, which bounds what is asked
/// for by what the input could decode to anyway: a caller that grows a sink for each item, as it would
/// without this, gets to the same sizes with a growth (and a copy) per doubling.
///
/// The list is walked from its start each time: a block costs one walk to find what it lacks and one to find
/// that the caller has made room, whatever the number of sinks. `visits` counts the entries that were looked
/// at, for [`Stats::reserve_visits`].
fn block_room(
    program: &Program,
    sinks: &[Sink<'_>],
    reserve: u32,
    count: u64,
    needs: &mut Vec<Need>,
    visits: &mut u64,
) -> bool {
    if reserve == NO_RESERVE || count < RESERVE_MIN_ITEMS {
        return false;
    }
    let mut found = false;
    for r in &program.reserves[reserve as usize..] {
        *visits += 1;
        if r.sink == NO_SINK {
            break;
        }
        let s = &sinks[r.sink as usize];
        let width = program.sinks[r.sink as usize].width() as u64;
        // An amount that does not fit 64 bits cannot be asked for: the room checks of the items decide.
        let lacks = (|| {
            let cap = (s.len as u64).checked_add(count.checked_mul(u64::from(r.elems))?)?;
            let cap2 = (s.len2 as u64).checked_add(count.checked_mul(u64::from(r.elems2))?)?;
            let lacks = cap.checked_mul(width)? > s.data.len() as u64
                || cap2.checked_mul(8)? > s.data2.len() as u64;
            Some(lacks.then_some(Need {
                sink: r.sink,
                min_cap: cap,
                min_cap2: cap2,
            }))
        })();
        match lacks {
            Some(Some(need)) => {
                if !found {
                    needs.clear();
                    found = true;
                }
                needs.push(need);
            }
            Some(None) => {}
            None => break,
        }
    }
    found
}

/// How many of the next `want` elements of width `width` bytes fit in the primary buffer of `s`.
#[inline(always)]
fn room_for(s: &Sink<'_>, width: usize, want: u64) -> usize {
    let room = (s.data.len() / width).saturating_sub(s.len);
    want.min(room as u64) as usize
}

/// Decodes up to `want` consecutive items of a loop whose body is the single op `body`, a number that is
/// written to a sink as it is read, and returns how many. Every item is what the op would have made of it
/// (the same bytes in the sink, the same bytes of the input consumed); it stops before the first one that
/// the op would not take without a word: a value that is not valid, or beyond the end of the input, or a
/// sink that is full. The dispatch of the executor decodes that item then, and stops or fails as it does.
/// Other bodies take no items.
///
/// Without the dispatch of two ops, the frame of the loop and the checks of the room for every item, this is
/// the loop that a block of a few thousand numbers spends its time in.
fn bulk_items(body: Op, input: &[u8], pos: &mut usize, sinks: &mut [Sink<'_>], want: u64) -> u64 {
    let mut done = 0usize;
    match body {
        Op::I64 {
            sink,
            chk: NO_CHECK,
        } => {
            let s = &mut sinks[sink as usize];
            let n = room_for(s, 8, want);
            while done < n {
                let Ok((v, len)) = decode_long(&input[*pos..]) else {
                    break;
                };
                s.data.put((s.len + done) * 8, &v.to_ne_bytes());
                *pos += len;
                done += 1;
            }
            s.len += done;
        }
        Op::I32 {
            sink,
            chk: NO_CHECK,
        } => {
            let s = &mut sinks[sink as usize];
            let n = room_for(s, 4, want);
            while done < n {
                let Ok((v, len)) = decode_int(&input[*pos..]) else {
                    break;
                };
                s.data.put((s.len + done) * 4, &v.to_ne_bytes());
                *pos += len;
                done += 1;
            }
            s.len += done;
        }
        Op::F64 { sink } => {
            let s = &mut sinks[sink as usize];
            done = room_for(s, 8, want).min((input.len() - *pos) / 8);
            if cfg!(target_endian = "little") {
                s.data.put(s.len * 8, &input[*pos..*pos + done * 8]);
            } else {
                for (i, b) in input[*pos..*pos + done * 8].chunks_exact(8).enumerate() {
                    let v = f64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
                    s.data.put((s.len + i) * 8, &v.to_ne_bytes());
                }
            }
            *pos += done * 8;
            s.len += done;
        }
        Op::F32 { sink } => {
            let s = &mut sinks[sink as usize];
            done = room_for(s, 4, want).min((input.len() - *pos) / 4);
            if cfg!(target_endian = "little") {
                s.data.put(s.len * 4, &input[*pos..*pos + done * 4]);
            } else {
                for (i, b) in input[*pos..*pos + done * 4].chunks_exact(4).enumerate() {
                    let v = f32::from_le_bytes([b[0], b[1], b[2], b[3]]);
                    s.data.put((s.len + i) * 4, &v.to_ne_bytes());
                }
            }
            *pos += done * 4;
            s.len += done;
        }
        Op::Bool { sink } => {
            let s = &mut sinks[sink as usize];
            let n = room_for(s, 1, want).min(input.len() - *pos);
            while done < n {
                let b = input[*pos + done];
                if b > 1 {
                    break;
                }
                s.data.put_byte(s.len + done, b);
                done += 1;
            }
            *pos += done;
            s.len += done;
        }
        _ => {}
    }
    done as u64
}

/// Big-endian two's complement `src` into a host-endian integer of `dst.len()`
/// bytes, sign extended. `false` when the value does not fit; `dst` is then untouched. Writes every byte of
/// `dst` and reads none.
fn decimal_to_host(src: &[u8], dst: &mut [MaybeUninit<u8>]) -> bool {
    let w = dst.len();
    let sign = if src.first().is_some_and(|b| b & 0x80 != 0) {
        0xffu8
    } else {
        0
    };
    let (head, tail) = if src.len() > w {
        src.split_at(src.len() - w)
    } else {
        (&src[..0], src)
    };
    // Extra leading bytes are only acceptable as pure sign extension.
    if head.iter().any(|&b| b != sign)
        || (!head.is_empty() && ((tail[0] & 0x80 != 0) != (sign == 0xff)))
    {
        return false;
    }
    for (i, d) in dst.iter_mut().enumerate() {
        d.write(if i < tail.len() {
            tail[tail.len() - 1 - i]
        } else {
            sign
        });
    }
    if cfg!(target_endian = "big") {
        dst.reverse();
    }
    true
}

fn write_offset(s: &mut Sink<'_>, add: u64) {
    s.offset = s.offset.wrapping_add(add);
    s.data.put(s.len * 8, &s.offset.to_ne_bytes());
    s.len += 1;
}

/// Index of the first value in the `Int32` buffer `data` (host byte order)
/// that is outside `[min, max]`. The date and datetime range checks of the C++
/// reader run as one pass like this over a decoded column.
pub fn first_out_of_range_i32(data: &[u8], min: i32, max: i32) -> Option<usize> {
    data.chunks_exact(4).position(|c| {
        let v = i32::from_ne_bytes([c[0], c[1], c[2], c[3]]);
        v < min || v > max
    })
}

/// A failed [`decode_partial`]: the error and what was committed before it.
#[derive(Debug, PartialEq, Eq)]
pub struct DecodeError {
    pub error: Error,
    /// Rows completed by this call before the error.
    pub rows_done: u64,
    /// Bytes of `input` they occupy. The failing row starts here.
    pub consumed: usize,
}

/// Decodes rows of `program` from `input` into `sinks`, transactionally per
/// row: except when a row is paused for space or input (below), the state is at a row
/// boundary afterwards and the sinks hold exactly the rows reported in `rows_done`.
///
/// * `Ok` with `RowLimit` or `BlockDone`: as before.
/// * `Ok` with `NeedSpace`, `rows_done == 0`: the first row of the call is *paused*, not discarded.
///   The state keeps where it stood (instruction, loops, input position, zero-size item budget) and the
///   sinks keep what the row has written so far, so their fill levels are *past* the row boundary. Grow
///   the sinks that [`State::needs`] lists, keeping every byte below their fill levels, and call again with
///   the same input (`&input[consumed..]` with `consumed == 0`) and the same state, with
///   [`decode_resumed`] if nothing but those sinks changed. The row continues where it stopped:
///   no byte is decoded twice, however many sinks have to grow. The reported capacities count from the
///   fill levels of the sinks.
/// * `Ok` with `NeedSpace`, `rows_done > 0`: `rows_done` and `consumed` cover the rows completed before
///   the row that needs more room. That row is discarded (the sinks' fill levels are rolled back to the
///   row boundary) and is the first row of the next call, `&input[consumed..]`, which pauses on every
///   further `NeedSpace`. The reported capacity is what the row needs, counted from the rolled back fill
///   level.
/// * `Ok` with `NeedInput` (streaming mode, see [`State::set_streaming`]): the input ended inside a row.
///   With `rows_done == 0` the row is paused as for `NeedSpace`, but the bytes of the row that the
///   executor is done with are *consumed*: call again with the input from `consumed` on, extended with
///   the bytes that follow it, and with the same sinks (nothing may change in their buffers). The row
///   continues at the start of the value that the input ended in, or, for a `bytes`, `string` or `fixed`
///   that is skipped, after the bytes of it that were skipped already. So a row is never held whole, and
///   a skipped value costs no more memory than the bytes that the caller has at a time. The call that
///   continues such a row completes it and stops (`RowLimit`, `rows_done == 1`, whatever `max_rows` is),
///   so that a later row never has to roll back into input that was dropped. With `rows_done > 0` the row
///   that was cut is discarded as for `NeedSpace` and decoded again by the next call.
/// * `Err`: the error, `rows_done` and `consumed` as above; the failing row is rolled back to the start
///   of the call, or to the start of the row if the call resumed a paused row. The state is reset, except
///   that an input which ends inside the row ([`ErrorCode::Eof`], [`ErrorCode::CountExceedsPayload`])
///   keeps the block's zero-size item budget as it stood after the completed rows, so that a caller which
///   resumes the row with more bytes does not get a fresh budget at every window boundary. Without
///   streaming mode that is what an input that ends inside a row is; a caller that has no more bytes
///   switches streaming off for its last call, which then reports the error for a paused row, too.
///
/// A row paused for space must be resumed with a prefix of the same bytes at least as long as where it
/// paused; [`State::reset`] abandons a paused row. Rolling back replays the completed rows of the call
/// into the same buffer positions, so it costs one more pass over them, and only on a `NeedSpace` or
/// `NeedInput` that is not in the first row of a call and on errors; a row is replayed at most once per
/// call it completes in. Bytes past the restored fill levels may hold stale data.
pub fn decode_partial(
    program: &Program,
    state: &mut State,
    input: &[u8],
    max_rows: u64,
    sinks: &mut [Sink<'_>],
) -> std::result::Result<Progress, DecodeError> {
    decode_checked(program, state, input, max_rows, sinks, Check::All)
}

/// [`decode_partial`] for the call that continues a row that the last call paused, when nothing but the
/// buffers of the sinks that [`State::needs`] named changed since: the other sinks are the same buffers with
/// the fill levels that the paused call left in them, which they were checked for when the row started. Only
/// the sinks that were to grow are checked again, so resuming costs the same whatever the number of sinks,
/// instead of one pass over all of them for every sink that has to grow. After [`Status::NeedInput`] no
/// buffer changed and none is checked.
///
/// Without a paused row in `state` this is an [`ErrorCode::InvalidArgument`] error.
pub fn decode_resumed(
    program: &Program,
    state: &mut State,
    input: &[u8],
    max_rows: u64,
    sinks: &mut [Sink<'_>],
) -> std::result::Result<Progress, DecodeError> {
    decode_checked(program, state, input, max_rows, sinks, Check::Resumed)
}

/// The sinks that a call validates before it runs.
#[derive(Clone, Copy)]
enum Check {
    /// All of them: the call may start a row.
    All,
    /// The ones that differ from the call that paused the row: the sinks of [`State::needs`], none after
    /// a pause for input.
    Resumed,
}

fn no_paused_row() -> Error {
    Error::new(ErrorCode::InvalidArgument, "no paused row to resume")
}

fn decode_checked(
    program: &Program,
    state: &mut State,
    input: &[u8],
    max_rows: u64,
    sinks: &mut [Sink<'_>],
    check: Check,
) -> std::result::Result<Progress, DecodeError> {
    let fail = |error: Error| DecodeError {
        error,
        rows_done: 0,
        consumed: 0,
    };
    let resume = state.paused.take();
    let checked = match check {
        Check::All => {
            state.stats.sinks_checked += sinks.len() as u64;
            check_sinks(program, sinks)
        }
        Check::Resumed => {
            state.stats.sinks_checked += state.needs.len() as u64;
            check_sink_count(program, sinks).and_then(|()| match resume {
                Some(_) => state.needs.iter().try_for_each(|need| {
                    let i = need.sink as usize;
                    if i < sinks.len() {
                        check_sink(program, sinks, i)
                    } else {
                        Err(Error::new(
                            ErrorCode::InvalidArgument,
                            format!("sink {i} does not exist"),
                        ))
                    }
                }),
                None => Err(no_paused_row()),
            })
        }
    }
    .and_then(|()| match resume {
        Some(pos) if pos > input.len() || state.fill.len() != sinks.len() => Err(Error::new(
            ErrorCode::InvalidArgument,
            "a paused row was resumed with less input or other sinks",
        )),
        _ => Ok(()),
    });
    if let Err(error) = checked {
        if resume.is_some() {
            state.reset();
        }
        return Err(fail(error));
    }
    if resume.is_none() {
        state.reset_row();
        state.fill.clear();
        state
            .fill
            .extend(sinks.iter().map(|s| (s.len, s.len2, s.offset)));
        state.zero_items_start = state.zero_items;
    }
    // A row that began in input that is gone is completed alone: see `Status::NeedInput`.
    let max_rows = if state.carried {
        max_rows.min(1)
    } else {
        max_rows
    };
    state.rows_call = 0;
    state.stats.calls += 1;
    let outcome = run(program, state, input, max_rows, sinks, resume);
    let pause_pos = outcome.as_ref().map_or(0, |p| p.consumed);
    let (rows_done, stop) = match outcome {
        Ok(
            p @ Progress {
                status: Status::BlockDone | Status::RowLimit,
                ..
            },
        ) => {
            state.carried = false;
            return Ok(p);
        }
        Ok(
            p @ Progress {
                rows_done: 0,
                status: Status::NeedSpace { .. },
                ..
            },
        ) => {
            // `run` saved the executor state at the start of the op that needs room.
            state.paused = Some(p.consumed);
            state.stats.need_space += 1;
            return Ok(Progress { consumed: 0, ..p });
        }
        Ok(
            p @ Progress {
                rows_done: 0,
                status: Status::NeedInput,
                ..
            },
        ) => {
            // `run` saved the executor state at the start of the op that the input ended in (or after the
            // skipped value that it ended in), and everything before it is consumed: the input of the next
            // call starts there.
            state.paused = Some(0);
            state.needs.clear();
            state.carried = true;
            state.stats.need_input += 1;
            return Ok(p);
        }
        Ok(p) => (p.rows_done, Ok(p.status)),
        Err(e) => (state.rows_call, Err(e)),
    };
    // Back to the start of the call (of the row, if it resumed a paused one), then replay the rows that
    // completed.
    for (s, f) in sinks.iter_mut().zip(&state.fill) {
        (s.len, s.len2, s.offset) = *f;
    }
    state.reset_row();
    state.zero_items = state.zero_items_start;
    let consumed = if rows_done == 0 {
        0
    } else {
        match run(program, state, input, rows_done, sinks, None) {
            Ok(p) if p.rows_done == rows_done && p.status == Status::RowLimit => p.consumed,
            _ => {
                state.reset();
                return Err(DecodeError {
                    error: Error::new(ErrorCode::Internal, "row replay diverged"),
                    rows_done: 0,
                    consumed: 0,
                });
            }
        }
    };
    state.stats.replayed_bytes += consumed as u64;
    match stop {
        Ok(status) => {
            // `pause_pos` is where the discarded row stopped: its bytes are decoded again by the next call.
            if status == Status::NeedInput {
                state.stats.need_input += 1;
            } else {
                state.stats.need_space += 1;
            }
            state.stats.restarts += 1;
            state.stats.replayed_bytes += (pause_pos - consumed) as u64;
            state.reset_row();
            Ok(Progress {
                rows_done,
                consumed,
                status,
            })
        }
        Err(error) => {
            if matches!(error.code, ErrorCode::Eof | ErrorCode::CountExceedsPayload) {
                state.reset_row();
            } else {
                state.reset();
            }
            Err(DecodeError {
                error,
                rows_done,
                consumed,
            })
        }
    }
}

/// [`decode_partial`] without the progress of a failed call.
pub fn decode(
    program: &Program,
    state: &mut State,
    input: &[u8],
    max_rows: u64,
    sinks: &mut [Sink<'_>],
) -> Result<Progress> {
    decode_partial(program, state, input, max_rows, sinks).map_err(|e| e.error)
}

/// The executor proper: may stop in the middle of a row (`NeedSpace`, `NeedInput`) or fail
/// with sinks and state half way through a row. `resume` is the position in
/// `input` of a row that an earlier call paused: its state is in `state`.
fn run(
    program: &Program,
    state: &mut State,
    input: &[u8],
    max_rows: u64,
    sinks: &mut [Sink<'_>],
    resume: Option<usize>,
) -> Result<Progress> {
    let ops = &program.ops[..];
    let streaming = state.streaming;
    let mut ip = state.ip as usize;
    let mut pos = resume.unwrap_or(0);
    let mut rows = 0u64;
    let mut resumed = resume.is_some();

    // Leaves `decode` with the state saved at the start of the current op. The sink that lacks room is the
    // only need.
    macro_rules! need_space {
        ($sink:expr, $cap:expr, $cap2:expr) => {{
            state.needs.clear();
            state.needs.push(Need {
                sink: $sink,
                min_cap: $cap as u64,
                min_cap2: $cap2 as u64,
            });
            need_space!();
        }};
        // The sinks of `state.needs` lack room.
        () => {{
            state.ip = ip as u32;
            let first = state.needs[0];
            return Ok(Progress {
                rows_done: rows,
                consumed: pos,
                status: Status::NeedSpace {
                    sink: first.sink,
                    min_cap: first.min_cap,
                    min_cap2: first.min_cap2,
                },
            });
        }};
    }
    // Leaves `decode` because the input ended inside the current op, with the state saved at the start of it
    // (`pos` and `ip` are those of the op: nothing of it has been changed). The op that is cut is decoded
    // again from its start, with the bytes that follow. Only used in streaming mode.
    macro_rules! need_input {
        () => {{
            state.ip = ip as u32;
            return Ok(Progress {
                rows_done: rows,
                consumed: pos,
                status: Status::NeedInput,
            });
        }};
    }
    // The input ending in the current op is a pause in streaming mode and the error that it is otherwise.
    macro_rules! or_pause {
        ($e:expr) => {
            match $e {
                Ok(v) => v,
                Err(e) => {
                    if streaming
                        && matches!(e.code, ErrorCode::Eof | ErrorCode::CountExceedsPayload)
                    {
                        need_input!();
                    }
                    return Err(e);
                }
            }
        };
    }
    macro_rules! varint {
        ($e:expr) => {
            or_pause!($e.map_err(VarintError::into_error))
        };
    }
    macro_rules! end_of_input {
        () => {{
            if streaming {
                need_input!();
            }
            return Err(eof());
        }};
    }

    // A row that paused inside a skipped value first skips what is left of it.
    if state.skip_left > 0 {
        let take = state.skip_left.min((input.len() - pos) as u64);
        // `take <= input.len() - pos`
        pos += take as usize;
        state.skip_left -= take;
        if state.skip_left > 0 {
            end_of_input!();
        }
    }

    let status = 'run: loop {
        if !resumed {
            if rows >= max_rows {
                break 'run Status::RowLimit;
            }
            if pos == input.len() && program.row_min > 0 {
                break 'run Status::BlockDone;
            }
        }
        resumed = false;
        loop {
            match ops[ip] {
                Op::I32 { sink, chk } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 4;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let (v, n) = varint!(decode_int(&input[pos..]));
                    if chk != NO_CHECK {
                        check_range(program, chk, i64::from(v))?;
                    }
                    s.data.put(end - 4, &v.to_ne_bytes());
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::I32As64 { sink, chk } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 8;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let (v, n) = varint!(decode_int(&input[pos..]));
                    if chk != NO_CHECK {
                        check_range(program, chk, i64::from(v))?;
                    }
                    s.data.put(end - 8, &i64::from(v).to_ne_bytes());
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::I64 { sink, chk } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 8;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let (v, n) = varint!(decode_long(&input[pos..]));
                    if chk != NO_CHECK {
                        check_range(program, chk, v)?;
                    }
                    s.data.put(end - 8, &v.to_ne_bytes());
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::F32 { sink } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 4;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let Some(b) = input[pos..].first_chunk::<4>() else {
                        end_of_input!();
                    };
                    s.data.put(end - 4, &f32::from_le_bytes(*b).to_ne_bytes());
                    s.len += 1;
                    pos += 4;
                    ip += 1;
                }
                Op::F32As64 { sink } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 8;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let Some(b) = input[pos..].first_chunk::<4>() else {
                        end_of_input!();
                    };
                    s.data
                        .put(end - 8, &f64::from(f32::from_le_bytes(*b)).to_ne_bytes());
                    s.len += 1;
                    pos += 4;
                    ip += 1;
                }
                Op::F64 { sink } => {
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * 8;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let Some(b) = input[pos..].first_chunk::<8>() else {
                        end_of_input!();
                    };
                    s.data.put(end - 8, &f64::from_le_bytes(*b).to_ne_bytes());
                    s.len += 1;
                    pos += 8;
                    ip += 1;
                }
                Op::Bool { sink } => {
                    let s = &mut sinks[sink as usize];
                    if s.len >= s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let Some(&b) = input.get(pos) else {
                        end_of_input!();
                    };
                    if b > 1 {
                        return Err(Error::new(ErrorCode::InvalidBool, "Invalid value for bool"));
                    }
                    s.data.put_byte(s.len, b);
                    s.len += 1;
                    pos += 1;
                    ip += 1;
                }
                Op::Bytes { sink, terminated } => {
                    let (start, l) = or_pause!(read_length(input, pos));
                    let s = &mut sinks[sink as usize];
                    let need = l + usize::from(terminated);
                    if s.len + need > s.data.len() || (s.len2 + 1) * 8 > s.data2.len() {
                        need_space!(sink, s.len + need, s.len2 + 1);
                    }
                    s.data.put(s.len, &input[start..start + l]);
                    if terminated {
                        s.data.put_byte(s.len + l, 0);
                    }
                    s.len += need;
                    s.offset = s.offset.wrapping_add(need as u64);
                    s.data2.put(s.len2 * 8, &s.offset.to_ne_bytes());
                    s.len2 += 1;
                    pos = start + l;
                    ip += 1;
                }
                Op::Fixed { sink, n } => {
                    let n = n as usize;
                    let s = &mut sinks[sink as usize];
                    // The input is checked before space is requested: a truncated `fixed` must fail
                    // as an EOF, not make the caller allocate `n` bytes for a value that is not there.
                    if input.len() - pos < n {
                        end_of_input!();
                    }
                    let end = (s.len + 1) * n;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    s.data.put(end - n, &input[pos..pos + n]);
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::DecBytes { sink, width } => {
                    let (start, l) = or_pause!(read_length(input, pos));
                    let w = usize::from(width);
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * w;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    if !decimal_to_host(&input[start..start + l], s.data.slot(end - w..end)) {
                        return Err(Error::new(
                            ErrorCode::DecimalTooWide,
                            format!("Avro decimal of {l} bytes does not fit in {w} bytes"),
                        ));
                    }
                    s.len += 1;
                    pos = start + l;
                    ip += 1;
                }
                Op::DecFixed { sink, n, width } => {
                    let (n, w) = (n as usize, usize::from(width));
                    let s = &mut sinks[sink as usize];
                    let end = (s.len + 1) * w;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    if input.len() - pos < n {
                        end_of_input!();
                    }
                    // The program compiler guarantees `n <= w`, so this always fits.
                    if !decimal_to_host(&input[pos..pos + n], s.data.slot(end - w..end)) {
                        return Err(Error::new(
                            ErrorCode::DecimalTooWide,
                            format!("Avro decimal of {n} bytes does not fit in {w} bytes"),
                        ));
                    }
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::Enum {
                    sink,
                    symbols,
                    width,
                } => {
                    let s = &mut sinks[sink as usize];
                    let w = usize::from(width);
                    let end = (s.len + 1) * w;
                    if end > s.data.len() {
                        need_space!(sink, s.len + 1, 0);
                    }
                    let (v, n) = varint!(decode_int(&input[pos..]));
                    if v < 0 || v as u32 >= symbols {
                        return Err(enum_error(v, symbols));
                    }
                    match w {
                        1 => s.data.put_byte(end - 1, v as u8),
                        2 => s.data.put(end - 2, &(v as u16).to_ne_bytes()),
                        _ => s.data.put(end - 4, &(v as u32).to_ne_bytes()),
                    }
                    s.len += 1;
                    pos += n;
                    ip += 1;
                }
                Op::SkipInt => {
                    let (_, n) = varint!(decode_int(&input[pos..]));
                    pos += n;
                    ip += 1;
                }
                Op::SkipLong => {
                    pos += varint!(varint_len(&input[pos..], false));
                    ip += 1;
                }
                Op::SkipBool => {
                    match input.get(pos) {
                        None => end_of_input!(),
                        Some(b) if *b > 1 => {
                            return Err(Error::new(
                                ErrorCode::InvalidBool,
                                "Invalid value for bool",
                            ))
                        }
                        Some(_) => {}
                    }
                    pos += 1;
                    ip += 1;
                }
                Op::SkipFixed { n } => {
                    let avail = (input.len() - pos) as u64;
                    if avail < n {
                        if streaming {
                            // What the input holds belongs to the value: it is skipped now, the rest of
                            // it in the input that follows, and the caller can drop all of it.
                            state.skip_left = n - avail;
                            pos = input.len();
                            ip += 1;
                            need_input!();
                        }
                        return Err(eof());
                    }
                    pos += n as usize;
                    ip += 1;
                }
                Op::SkipString => {
                    let (start, l) = or_pause!(read_len(input, pos));
                    let avail = input.len() - start;
                    if avail < l {
                        if streaming {
                            // As for `SkipFixed`; the length is read and need not be read again.
                            state.skip_left = (l - avail) as u64;
                            pos = input.len();
                            ip += 1;
                            need_input!();
                        }
                        return Err(eof());
                    }
                    pos = start + l;
                    ip += 1;
                }
                Op::SkipEnum { symbols } => {
                    let (v, n) = varint!(decode_int(&input[pos..]));
                    if v < 0 || v as u32 >= symbols {
                        return Err(enum_error(v, symbols));
                    }
                    pos += n;
                    ip += 1;
                }
                Op::Union {
                    sink,
                    mode,
                    n,
                    table,
                } => {
                    let (idx, len) = varint!(decode_long(&input[pos..]));
                    if idx < 0 || idx >= i64::from(n) {
                        return Err(Error::new(
                            ErrorCode::UnionIndexOutOfRange,
                            format!("Union index out of boundary: got {idx}, the number of branches is {n}"),
                        ));
                    }
                    match mode {
                        UnionMode::None => {}
                        UnionMode::Tag(w) => {
                            let s = &mut sinks[sink as usize];
                            let w = usize::from(w);
                            let end = (s.len + 1) * w;
                            if end > s.data.len() {
                                need_space!(sink, s.len + 1, 0);
                            }
                            match w {
                                1 => s.data.put_byte(end - 1, idx as u8),
                                2 => s.data.put(end - 2, &(idx as u16).to_ne_bytes()),
                                _ => s.data.put(end - 4, &(idx as u32).to_ne_bytes()),
                            }
                            s.len += 1;
                        }
                        UnionMode::NullMap(null_branch) => {
                            let s = &mut sinks[sink as usize];
                            if s.len >= s.data.len() {
                                need_space!(sink, s.len + 1, 0);
                            }
                            s.data
                                .put_byte(s.len, u8::from(idx == i64::from(null_branch)));
                            s.len += 1;
                        }
                    }
                    pos += len;
                    ip = program.jump_table[table as usize + idx as usize] as usize;
                }
                Op::Jump { to } => ip = to as usize,
                Op::LoopBegin {
                    sink,
                    item_min,
                    end,
                } => {
                    if sink != NO_SINK {
                        let s = &sinks[sink as usize];
                        if (s.len + 1) * 8 > s.data.len() {
                            need_space!(sink, s.len + 1, 0);
                        }
                    }
                    let (count, after) = or_pause!(read_block_count(input, pos));
                    // Before anything of the block is consumed, so that a pause for room starts the header
                    // again. Only a block that its bytes can hold is asked for: the rest fails below.
                    if item_min > 0 && count <= (input.len() - after) as u64 / item_min {
                        if let Op::LoopNext { reserve, .. } = ops[end as usize - 1] {
                            if block_room(
                                program,
                                sinks,
                                reserve,
                                count,
                                &mut state.needs,
                                &mut state.stats.reserve_visits,
                            ) {
                                need_space!();
                            }
                        }
                    }
                    if count == 0 {
                        pos = after;
                        if sink != NO_SINK {
                            write_offset(&mut sinks[sink as usize], 0);
                        }
                        ip = end as usize;
                        continue;
                    }
                    // With more input to come, a count that the window cannot hold is not a reason to wait for it:
                    // every item takes at least `item_min` bytes of input, so the work is bounded by the bytes
                    // that arrive, and a block that is too short ends in an EOF in the last window, where the
                    // check is exact. Items that take no bytes keep their budget.
                    if !(streaming && item_min > 0) {
                        or_pause!(check_block(
                            count,
                            item_min,
                            input.len() - after,
                            &mut state.zero_items
                        ));
                    }
                    pos = after;
                    if state.frames.len() >= state.max_depth {
                        return Err(too_deep());
                    }
                    state.frames.push(ExecFrame::Loop {
                        remaining: count,
                        total: 0,
                    });
                    ip += 1;
                }
                Op::LoopNext { begin, reserve } => {
                    let Some(ExecFrame::Loop { remaining, total }) = state.frames.last_mut() else {
                        return Err(Error::new(ErrorCode::Internal, "loop frame missing"));
                    };
                    if *remaining > 1 {
                        // A loop whose body is one number (its op at `begin + 1`, and `ip` the op after it)
                        // takes the items that follow in one run. It stops at the first one that it cannot
                        // take (bytes that are not a value, the end of the input, a full sink), and the
                        // dispatch below decodes that one and reports what is wrong with it.
                        if ip == begin as usize + 2 {
                            let done = bulk_items(
                                ops[begin as usize + 1],
                                input,
                                &mut pos,
                                sinks,
                                *remaining - 1,
                            );
                            *remaining -= done;
                            *total += done;
                            state.stats.bulk_items += done;
                        }
                    }
                    if *remaining > 1 {
                        *remaining -= 1;
                        *total += 1;
                        ip = begin as usize + 1;
                        continue;
                    }
                    // The last item of the block: the header of the next one follows. Nothing is changed
                    // until it is read and checked, so that the op can pause at its start.
                    let total = *total + 1;
                    let (count, after) = or_pause!(read_block_count(input, pos));
                    let Op::LoopBegin {
                        sink,
                        item_min,
                        end,
                    } = ops[begin as usize]
                    else {
                        return Err(Error::new(ErrorCode::Internal, "loop begin missing"));
                    };
                    // The header may announce a block that needs room.
                    if reserve != NO_RESERVE
                        && item_min > 0
                        && count <= (input.len() - after) as u64 / item_min
                        && block_room(
                            program,
                            sinks,
                            reserve,
                            count,
                            &mut state.needs,
                            &mut state.stats.reserve_visits,
                        )
                    {
                        need_space!();
                    }
                    if count == 0 {
                        state.frames.pop();
                        pos = after;
                        if sink != NO_SINK {
                            write_offset(&mut sinks[sink as usize], total);
                        }
                        ip = end as usize;
                        continue;
                    }
                    if !(streaming && item_min > 0) {
                        or_pause!(check_block(
                            count,
                            item_min,
                            input.len() - after,
                            &mut state.zero_items
                        ));
                    }
                    pos = after;
                    if let Some(ExecFrame::Loop {
                        remaining,
                        total: items,
                    }) = state.frames.last_mut()
                    {
                        *remaining = count;
                        *items = total;
                    }
                    ip = begin as usize + 1;
                }
                Op::Call { to } => {
                    if state.frames.len() >= state.max_depth {
                        return Err(too_deep());
                    }
                    state.frames.push(ExecFrame::Call {
                        ret: (ip + 1) as u32,
                    });
                    ip = to as usize;
                }
                Op::Return => {
                    let Some(ExecFrame::Call { ret }) = state.frames.pop() else {
                        return Err(Error::new(ErrorCode::Internal, "call frame missing"));
                    };
                    ip = ret as usize;
                }
                Op::EndRow => {
                    rows += 1;
                    state.rows_call = rows;
                    ip = 0;
                    // The budget spans the entire block, including subsequent calls.
                    break;
                }
            }
        }
    };
    state.ip = ip as u32;
    Ok(Progress {
        rows_done: rows,
        consumed: pos,
        status,
    })
}

/// Reads an Avro `int` length at `pos`: the start of the payload and its length. Does not look at the payload.
#[inline]
fn read_len(input: &[u8], pos: usize) -> Result<(usize, usize)> {
    let (l, n) = decode_int(&input[pos..]).map_err(VarintError::into_error)?;
    if l < 0 {
        return Err(Error::new(
            ErrorCode::NegativeLength,
            format!("Cannot have negative length: {l}"),
        ));
    }
    Ok((pos + n, l as usize))
}

/// Reads an Avro `int` length at `pos` and checks that that many bytes follow.
/// Returns the start of the payload and its length.
#[inline]
fn read_length(input: &[u8], pos: usize) -> Result<(usize, usize)> {
    let (start, l) = read_len(input, pos)?;
    if input.len() - start < l {
        return Err(eof());
    }
    Ok((start, l))
}

fn enum_error(v: i32, symbols: u32) -> Error {
    Error::new(
        ErrorCode::EnumIndexOutOfRange,
        format!("Avro enum index {v} is out of range, the schema has {symbols} symbols"),
    )
}

#[cold]
fn range_error(v: i64, min: i64, max: i64) -> Error {
    Error::new(
        ErrorCode::ValueOutOfRange,
        format!("Value {v} is out of range [{min}, {max}]"),
    )
}

#[inline]
fn check_range(program: &Program, chk: u32, v: i64) -> Result<()> {
    let (min, max) = program.checks[chk as usize];
    if v < min || v > max {
        return Err(range_error(v, min, max));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_count_check_uses_exact_item_size_above_u32() {
        // Items of at least 2^40 bytes: only two fit in 2^41 + 1 bytes. A size clamped to
        // u32::MAX would let 512 of them through.
        let item_min = 1u64 << 40;
        let remaining = (1usize << 41) + 1;
        let mut zero_items = 0;
        assert!(check_block(2, item_min, remaining, &mut zero_items).is_ok());
        assert_eq!(
            check_block(3, item_min, remaining, &mut zero_items)
                .unwrap_err()
                .code,
            ErrorCode::CountExceedsPayload
        );
    }
}
