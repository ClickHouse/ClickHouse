#pragma once

/// Used in ProfileEvents and MemoryTracker to determine their hierarchy level
/// The less value the higher level (zero level is the root)
enum class VariableContext
{
    Global = 0,
    User,           /// Group of processes
    Process,        /// For example, a query or a merge
    Thread,         /// A thread of a process
    Snapshot        /// Does not belong to anybody
};
