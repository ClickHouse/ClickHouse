#pragma once

#include <Storages/MergeTree/MergeTreeData.h>


namespace DB
{

class MergeTreePartChecker
{
public:
    struct Settings
    {
        bool verbose = false; /// Writes progress and errors to stderr, and does not stop at the first error.
        bool require_checksums = false; /// Requires column.txt to be.
        bool require_column_files = false; /// Requires that all columns from columns.txt have files.
        size_t index_granularity = 8192;

        Settings & setVerbose(bool verbose_) { verbose = verbose_; return *this; }
        Settings & setRequireChecksums(bool require_checksums_) { require_checksums = require_checksums_; return *this; }
        Settings & setRequireColumnFiles(bool require_column_files_) { require_column_files = require_column_files_; return *this; }
        Settings & setIndexGranularity(size_t index_granularity_) { index_granularity = index_granularity_; return *this; }
    };

    /** Completely checks the part data
      *  - Calculates checksums and compares them with checksums.txt.
      * - For arrays and strings, checks the correspondence of the size and amount of data.
      * - Checks the correctness of marks.
      * Throws an exception if the part is corrupted or if the check fails (TODO: you can try to separate these cases).
      */
    static void checkDataPart(
        String path,
        const Settings & settings,
        const DataTypes & primary_key_data_types,    /// Check the primary key. If it is not necessary, pass an empty array.
        MergeTreeData::DataPart::Checksums * out_checksums = nullptr,
        std::atomic<bool> * is_cancelled = nullptr);
};

}
