use std::fs::OpenOptions;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use memmap2::{MmapMut, MmapOptions};

#[repr(C)]
struct CacheStats {
    version: u8,

    cache_hits_local: AtomicUsize,
    cache_hits_remote: AtomicUsize,
    cache_miss: AtomicUsize,
    uncacheable: AtomicUsize,

    invocation_count: AtomicUsize,
}

const SHM_SIZE: u64 = std::mem::size_of::<CacheStats>() as u64;

pub struct CacheStatsTracker {
    #[allow(dead_code)]
    mmap: MmapMut,
    pointer: *mut CacheStats,
}

impl CacheStatsTracker {
    pub fn init() -> Self {
        let local_path = xdg::BaseDirectories::with_prefix("chcache");

        let mut stats_file_path = local_path
            .get_cache_home()
            .expect("Failed to find XDG_CACHE_HOME");
        stats_file_path.push("stats");

        let err = format!(
            "Failed to open statistics file {}",
            stats_file_path.display()
        );
        let stats_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(stats_file_path)
            .expect(&err);

        stats_file
            .set_len(SHM_SIZE)
            .expect("Can not set statistics file size");

        let mut mmap = unsafe {
            MmapOptions::new()
                .map_mut(&stats_file)
                .expect("Failed to mmap() statistics file")
        };

        let cache_stats = mmap.as_mut_ptr() as *mut CacheStats;

        unsafe {
            if ptr::read(cache_stats)
                .invocation_count
                .load(Ordering::Acquire)
                == 0
            {
                ptr::write(
                    cache_stats,
                    CacheStats {
                        version: 0,

                        cache_hits_local: AtomicUsize::new(0),
                        cache_hits_remote: AtomicUsize::new(0),
                        cache_miss: AtomicUsize::new(0),
                        uncacheable: AtomicUsize::new(0),

                        invocation_count: AtomicUsize::new(1),
                    },
                )
            }
        };

        CacheStatsTracker {
            mmap,
            pointer: cache_stats,
        }
    }

    #[inline]
    fn stats(&self) -> &CacheStats {
        unsafe {
            self.pointer
                .as_ref()
                .expect("Failed to get reference to mapped stats file")
        }
    }

    pub fn increment_invocation(&self) {
        self.stats()
            .invocation_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_local_hit(&self) {
        self.stats()
            .cache_hits_local
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_remote_hit(&self) {
        self.stats()
            .cache_hits_remote
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_miss(&self) {
        self.stats().cache_miss.fetch_add(1, Ordering::Relaxed);
    }

    pub fn increment_uncacheable(&self) {
        self.stats().uncacheable.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dump(&self) {
        println!("Statistics summary:");
        println!(
            "  {:<30} {}",
            "invocations",
            self.stats().invocation_count.load(Ordering::Relaxed)
        );
        println!(
            "  {:<30} {}",
            "cache hit (local)",
            self.stats().cache_hits_local.load(Ordering::Relaxed)
        );
        println!(
            "  {:<30} {}",
            "cache hit (remote)",
            self.stats().cache_hits_remote.load(Ordering::Relaxed)
        );
        println!(
            "  {:<30} {}",
            "cache miss",
            self.stats().cache_miss.load(Ordering::Relaxed)
        );
        println!(
            "  {:<30} {}",
            "uncacheable",
            self.stats().uncacheable.load(Ordering::Relaxed)
        );
    }
}
