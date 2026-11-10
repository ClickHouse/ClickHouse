#![forbid(unsafe_code)]

use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum StatusCode {
    Ok = 0,
    ChunkReady = 1,
    UnitDone = 2,
    Cancelled = 3,

    ErrInvalidArg = -1,
    ErrElf = -2,
    ErrMissingRequiredSection = -3,
    ErrDwarf = -4,
    ErrMissingAuxSection = -5,
    ErrCallback = -6,
    ErrInternal = -7,
}

#[derive(Debug, Clone)]
pub struct DwarfError {
    pub code: StatusCode,
    pub message: String,
}

impl DwarfError {
    pub fn invalid_arg(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrInvalidArg,
            message: msg.into(),
        }
    }

    pub fn elf(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrElf,
            message: msg.into(),
        }
    }

    pub fn missing_required_section(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrMissingRequiredSection,
            message: msg.into(),
        }
    }

    pub fn dwarf(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrDwarf,
            message: msg.into(),
        }
    }

    pub fn missing_aux_section(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrMissingAuxSection,
            message: msg.into(),
        }
    }

    pub fn callback(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrCallback,
            message: msg.into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            code: StatusCode::ErrInternal,
            message: msg.into(),
        }
    }

    pub fn cancelled() -> Self {
        Self {
            code: StatusCode::Cancelled,
            message: String::new(),
        }
    }
}

impl fmt::Display for DwarfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

pub type Result<T, E = DwarfError> = core::result::Result<T, E>;
