#![forbid(unsafe_code)]

//! Cooperative cancellation: the parser calls back into its owner at least every
//! [`WORK_QUANTUM`] work items (DIEs, attributes, range records) and before large reservations.

use crate::error::{DwarfError, Result};

/// Maximum number of work items between two polls of the owner.
pub const WORK_QUANTUM: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Poll {
    Continue,
    Stop,
}

/// Implemented by the FFI layer around `ChDwarfCancelFn`. `Err` means the callback failed; the
/// owner holds the exception and rethrows it.
pub trait Control {
    fn poll(&mut self) -> Result<Poll>;
}

pub struct Work<'c> {
    ctl: &'c mut dyn Control,
    pending: u32,
}

impl<'c> Work<'c> {
    pub fn new(ctl: &'c mut dyn Control) -> Self {
        Self { ctl, pending: 0 }
    }

    /// Accounts `n` work items; polls the owner when a quantum has accumulated.
    #[inline]
    pub fn tick(&mut self, n: usize) -> Result<()> {
        self.pending = self
            .pending
            .saturating_add(u32::try_from(n).unwrap_or(u32::MAX));
        if self.pending >= WORK_QUANTUM {
            self.check()
        } else {
            Ok(())
        }
    }

    /// Polls the owner now. A `Stop` answer is reported as the `Cancelled` status.
    pub fn check(&mut self) -> Result<()> {
        self.pending = 0;
        match self.ctl.poll()? {
            Poll::Continue => Ok(()),
            Poll::Stop => Err(DwarfError::cancelled()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StatusCode;

    struct Counting {
        polls: u32,
        stop_after: u32,
    }

    impl Control for Counting {
        fn poll(&mut self) -> Result<Poll> {
            self.polls += 1;
            Ok(if self.polls >= self.stop_after {
                Poll::Stop
            } else {
                Poll::Continue
            })
        }
    }

    #[test]
    fn polls_once_per_quantum() {
        let mut c = Counting {
            polls: 0,
            stop_after: u32::MAX,
        };
        {
            let mut w = Work::new(&mut c);
            for _ in 0..(3 * WORK_QUANTUM as usize + 5) {
                w.tick(1).unwrap();
            }
        }
        assert_eq!(c.polls, 3);
    }

    #[test]
    fn stop_maps_to_cancelled() {
        let mut c = Counting {
            polls: 0,
            stop_after: 1,
        };
        let mut w = Work::new(&mut c);
        let err = w.tick(WORK_QUANTUM as usize).unwrap_err();
        assert_eq!(err.code, StatusCode::Cancelled);
    }
}
