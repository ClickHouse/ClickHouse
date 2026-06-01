-- Tags: linux
-- linux: MemoryThreadStacks* metrics are read from /proc/self/smaps, available on Linux only.

-- All three metrics are exposed.
SELECT count() = 3
FROM system.asynchronous_metrics
WHERE metric IN ('MemoryThreadStacksCount', 'MemoryThreadStacksVirtual', 'MemoryThreadStacksResident');

-- Count is positive: the server has at least the main thread plus a handful
-- of pool / listener threads, and the metric should account for most of them.
SELECT value > 4
FROM system.asynchronous_metrics
WHERE metric = 'MemoryThreadStacksCount';

-- Virtual is positive and consistent with the count: each registered stack
-- contributes at least one OS page of virtual address space.
-- Using a permissive lower bound (16 KiB per stack) to avoid flakiness on
-- exotic kernel page sizes; the typical x86_64 value is 8 MiB per stack.
WITH
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksCount') AS cnt,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksVirtual') AS virt
SELECT virt >= cnt * 16384;

-- Resident is non-negative and bounded by Virtual (Rss <= Size for every VMA
-- in /proc/self/smaps, so this holds when summed across matching VMAs).
WITH
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksVirtual') AS virt,
    (SELECT value FROM system.asynchronous_metrics WHERE metric = 'MemoryThreadStacksResident') AS rss
SELECT rss >= 0 AND rss <= virt;
