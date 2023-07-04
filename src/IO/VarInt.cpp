#include <IO/VarInt.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int BAD_ARGUMENTS;
}

void throwReadAfterEOF()
{
    throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Attempt to read after eof");
}

void throwValueTooLargeForVarIntEncoding(UInt64 x)
{
    /// Under practical circumstances, we should virtually never end up here but AST Fuzzer manages to create superlarge input integers
    /// which trigger this exception. Intentionally not throwing LOGICAL_ERROR or calling abort() or [ch]assert(false), so AST Fuzzer
    /// can swallow the exception and continue to run.
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value {} is too large for VarInt encoding", x);
}

}
