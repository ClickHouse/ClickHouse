#pragma once

namespace DB
{

/// A result code for the KILL QUERY/KILL MUTATION statement.
enum class CancellationCode : uint8_t
{
    NotFound = 0,                     /// already cancelled
    QueryIsNotInitializedYet = 1,
    CancelCannotBeSent = 2,
    CancelSent = 3,
    Unknown = 255
};

}
