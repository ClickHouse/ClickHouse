set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/quill")

add_library(_quill INTERFACE)

target_include_directories(_quill SYSTEM INTERFACE "${LIBRARY_DIR}/include")

add_library(ch_contrib::quill ALIAS _quill)

# header files
set(HEADER_FILES
        ${LIBRARY_DIR}/include/quill/backend/BackendManager.h
        ${LIBRARY_DIR}/include/quill/backend/BackendOptions.h
        ${LIBRARY_DIR}/include/quill/backend/BackendWorker.h
        ${LIBRARY_DIR}/include/quill/backend/BacktraceStorage.h
        ${LIBRARY_DIR}/include/quill/backend/ManualBackendWorker.h
        ${LIBRARY_DIR}/include/quill/backend/PatternFormatter.h
        ${LIBRARY_DIR}/include/quill/backend/RdtscClock.h
        ${LIBRARY_DIR}/include/quill/backend/SignalHandler.h
        ${LIBRARY_DIR}/include/quill/backend/StringFromTime.h
        ${LIBRARY_DIR}/include/quill/backend/ThreadUtilities.h
        ${LIBRARY_DIR}/include/quill/backend/TimestampFormatter.h
        ${LIBRARY_DIR}/include/quill/backend/TransitEvent.h
        ${LIBRARY_DIR}/include/quill/backend/TransitEventBuffer.h
        ${LIBRARY_DIR}/include/quill/backend/BackendUtilities.h

        ${LIBRARY_DIR}/include/quill/bundled/fmt/args.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/base.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/chrono.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/color.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/compile.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/format.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/format-inl.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/os.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/ostream.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/printf.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/ranges.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/std.h
        ${LIBRARY_DIR}/include/quill/bundled/fmt/xchar.h

        ${LIBRARY_DIR}/include/quill/core/Attributes.h
        ${LIBRARY_DIR}/include/quill/core/BoundedSPSCQueue.h
        ${LIBRARY_DIR}/include/quill/core/Common.h
        ${LIBRARY_DIR}/include/quill/core/DynamicFormatArgStore.h
        ${LIBRARY_DIR}/include/quill/core/Codec.h
        ${LIBRARY_DIR}/include/quill/core/Filesystem.h
        ${LIBRARY_DIR}/include/quill/core/FrontendOptions.h
        ${LIBRARY_DIR}/include/quill/core/InlinedVector.h
        ${LIBRARY_DIR}/include/quill/core/LoggerBase.h
        ${LIBRARY_DIR}/include/quill/core/LoggerManager.h
        ${LIBRARY_DIR}/include/quill/core/LogLevel.h
        ${LIBRARY_DIR}/include/quill/core/MacroMetadata.h
        ${LIBRARY_DIR}/include/quill/core/MathUtilities.h
        ${LIBRARY_DIR}/include/quill/core/PatternFormatterOptions.h
        ${LIBRARY_DIR}/include/quill/core/QuillError.h
        ${LIBRARY_DIR}/include/quill/core/Rdtsc.h
        ${LIBRARY_DIR}/include/quill/core/SinkManager.h
        ${LIBRARY_DIR}/include/quill/core/Spinlock.h
        ${LIBRARY_DIR}/include/quill/core/ThreadContextManager.h
        ${LIBRARY_DIR}/include/quill/core/TimeUtilities.h
        ${LIBRARY_DIR}/include/quill/core/UnboundedSPSCQueue.h
        ${LIBRARY_DIR}/include/quill/backend/Utf8Conv.h

        ${LIBRARY_DIR}/include/quill/filters/Filter.h

        ${LIBRARY_DIR}/include/quill/sinks/ConsoleSink.h
        ${LIBRARY_DIR}/include/quill/sinks/FileSink.h
        ${LIBRARY_DIR}/include/quill/sinks/JsonSink.h
        ${LIBRARY_DIR}/include/quill/sinks/NullSink.h
        ${LIBRARY_DIR}/include/quill/sinks/RotatingFileSink.h
        ${LIBRARY_DIR}/include/quill/sinks/RotatingJsonFileSink.h
        ${LIBRARY_DIR}/include/quill/sinks/RotatingSink.h
        ${LIBRARY_DIR}/include/quill/sinks/Sink.h
        ${LIBRARY_DIR}/include/quill/sinks/StreamSink.h

        ${LIBRARY_DIR}/include/quill/std/Array.h
        ${LIBRARY_DIR}/include/quill/std/Chrono.h
        ${LIBRARY_DIR}/include/quill/std/Deque.h
        ${LIBRARY_DIR}/include/quill/std/FilesystemPath.h
        ${LIBRARY_DIR}/include/quill/std/ForwardList.h
        ${LIBRARY_DIR}/include/quill/std/List.h
        ${LIBRARY_DIR}/include/quill/std/Map.h
        ${LIBRARY_DIR}/include/quill/std/Optional.h
        ${LIBRARY_DIR}/include/quill/std/Pair.h
        ${LIBRARY_DIR}/include/quill/std/Set.h
        ${LIBRARY_DIR}/include/quill/std/Tuple.h
        ${LIBRARY_DIR}/include/quill/std/UnorderedMap.h
        ${LIBRARY_DIR}/include/quill/std/UnorderedSet.h
        ${LIBRARY_DIR}/include/quill/std/Vector.h
        ${LIBRARY_DIR}/include/quill/std/WideString.h

        ${LIBRARY_DIR}/include/quill/Backend.h
        ${LIBRARY_DIR}/include/quill/BackendTscClock.h
        ${LIBRARY_DIR}/include/quill/CsvWriter.h
        ${LIBRARY_DIR}/include/quill/DeferredFormatCodec.h
        ${LIBRARY_DIR}/include/quill/DirectFormatCodec.h
        ${LIBRARY_DIR}/include/quill/Frontend.h
        ${LIBRARY_DIR}/include/quill/Logger.h
        ${LIBRARY_DIR}/include/quill/LogMacros.h
        ${LIBRARY_DIR}/include/quill/StopWatch.h
        ${LIBRARY_DIR}/include/quill/StringRef.h
        ${LIBRARY_DIR}/include/quill/TriviallyCopyableCodec.h
        ${LIBRARY_DIR}/include/quill/UserClockSource.h
        ${LIBRARY_DIR}/include/quill/Utility.h
)

target_sources(_quill PRIVATE ${HEADER_FILES})