#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>

#include <optional>

namespace DB
{

class Context;

/** Implements a table engine for the system table "numbers".
  * The table contains the only column number UInt64.
  * From this table, you can read all natural numbers, starting from 0 (to 2^64 - 1, and then again).
  *
  * You could also specify a limit (how many numbers to give).
  *
  * How to generate numbers?
  *
  * 1. First try a smart fashion:
  *
  * In this fashion we try to push filters and limit down to scanning.
  * Firstly extract plain ranges(no overlapping and ordered) by filter expressions.
  *
  * For example:
  *     where (numbers > 1 and numbers < 3) or (numbers in (4, 6)) or (numbers > 7 and numbers < 9)
  *
  * We will get ranges
  *     (1, 3), [4, 4], [6, 6], (7, 9)
  *
  * Then split the ranges evenly to one or multi-streams. With this way we will get result without large scanning.
  *
  * 2. If fail to extract plain ranges, fall back to ordinary scanning.
  *
  * If multithreaded is specified, numbers will be generated in several streams
  *  (and result could be out of order). If both multithreaded and limit are specified,
  *  the table could give you not exactly 1..limit range, but some arbitrary 'limit' numbers.
  */

class StorageSystemNumbers final : public StorageWithCommonVirtualColumns
{
public:
    /// `limit` is a domain window size, not the output row count
    /// (e.g. `numbers(10, 5, 2)` has limit=5, offset=10, step=2 — the domain is
    /// 5 consecutive integers [10, 14], but only 3 values are produced: 10, 12, 14).
    /// Stored as UInt128 so that a full 2^64-element domain (e.g. generate_series(0, UInt64_MAX))
    /// can be represented without overflow.
    /// When `descending` is true, `offset` is the first (largest) value and the series
    /// counts downward; `step` is always the positive absolute value.
    StorageSystemNumbers(
        const StorageID & table_id,
        bool multithreaded_,
        const std::string & column_name,
        std::optional<UInt128> limit_ = std::nullopt,
        UInt64 offset_ = 0,
        UInt64 step_ = 1,
        bool descending_ = false);

    std::string getName() const override { return "SystemNumbers"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams) override;

    bool hasEvenlyDistributedRead() const override { return true; }
    bool isSystemStorage() const override { return true; }
    bool supportsTransactions() const override { return true; }

private:
    friend class ReadFromSystemNumbersStep;

    bool multithreaded;
    std::optional<UInt128> limit;
    UInt64 offset;
    std::string column_name;
    UInt64 step;

    /// When true, the series is descending (e.g. `generate_series(10, 0, -1)`).
    /// `offset` holds the first (largest) value; `step` is the positive absolute value.
    /// Uses `SimpleSteppedNumbersSource` and does not support filter pushdown.
    bool descending = false;
};

}
