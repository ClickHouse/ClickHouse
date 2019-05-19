#include "QueryProfiler.h"

namespace DB
{

LazyPipe trace_pipe;

void CloseQueryTraceStream() {
    DB::WriteBufferFromFileDescriptor out(trace_pipe.fds_rw[1]);
    DB::writeIntBinary(true, out);
    out.next();
}

}
