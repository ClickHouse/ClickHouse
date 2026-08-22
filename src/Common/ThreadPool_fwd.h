#pragma once

template <typename Thread>
class ThreadPoolImpl;

template <bool propagate_opentelemetry_context, bool global_trace_collector_allowed>
class ThreadFromGlobalPoolImpl;

using ThreadFromGlobalPoolNoTracingContextPropagation = ThreadFromGlobalPoolImpl<false, true>;

using ThreadFromGlobalPool = ThreadFromGlobalPoolImpl<true, true>;
using ThreadFromGlobalPoolWithoutTraceCollector = ThreadFromGlobalPoolImpl<true, false>;

using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolNoTracingContextPropagation>;
