#pragma once

namespace DB
{
    /// Types of data part format.
    enum class MergeTreeDataPartType
    {
        /// Data of each is stored in one or several (for complex types) files.
        /// Every data file is followed by marks file.
        WIDE,

        /// Data of all columns is stored in one file. Marks are also stored in single file.
        COMPACT,

        /// Format with buffering data in RAM. Not implemented yet.
        IN_MEMORY,

        UNKNOWN,
    };
}
