#pragma once

template <typename Thread>
class ThreadPoolImpl;

template <bool propagate_opentelemetry_context>
class ThreadFromGlobalPoolImpl;

using ThreadFromGlobalPoolNoTracingContextPropagation = ThreadFromGlobalPoolImpl<false>;

using ThreadFromGlobalPool = ThreadFromGlobalPoolImpl<true>;

using ThreadPool = ThreadPoolImpl<ThreadFromGlobalPoolNoTracingContextPropagation>;
