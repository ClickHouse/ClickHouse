#include <daemon/SentryWriter.h>

#include <common/getFQDNOrHostName.h>
#if !defined(ARCADIA_BUILD)
#   include "Common/config_version.h"
#endif

#include <sentry.h>

namespace {
    void setExtras() {
        sentry_set_extra("version_githash", sentry_value_new_string(VERSION_GITHASH));
        sentry_set_extra("version_describe", sentry_value_new_string(VERSION_DESCRIBE));
        sentry_set_extra("version_integer", sentry_value_new_int32(VERSION_INTEGER));
        sentry_set_extra("version_revision", sentry_value_new_int32(VERSION_REVISION));
        sentry_set_extra("version_major", sentry_value_new_int32(VERSION_MAJOR));
        sentry_set_extra("version_minor", sentry_value_new_int32(VERSION_MINOR));
        sentry_set_extra("version_patch", sentry_value_new_int32(VERSION_PATCH));
    }
}

void SentryWriter::initialize() {
    sentry_options_t * options = sentry_options_new();
    sentry_options_set_release(options, VERSION_STRING);
    sentry_options_set_debug(options, 1);
    sentry_init(options);
    sentry_options_set_dsn(options, "https://6f33034cfe684dd7a3ab9875e57b1c8d@o388870.ingest.sentry.io/5226277");
    if (strstr(VERSION_DESCRIBE, "-stable") || strstr(VERSION_DESCRIBE, "-lts")) {
        sentry_options_set_environment(options, "prod");
    } else {
        sentry_options_set_environment(options, "test");
    }
}

void SentryWriter::shutdown() {
    sentry_shutdown();
}

void SentryWriter::onFault(
    int sig,
    const siginfo_t & info,
    const ucontext_t & context,
    const StackTrace & stack_trace
    )
{
    const std::string & error_message = signalToErrorMessage(sig, info, context);
    sentry_value_t event = sentry_value_new_message_event(SENTRY_LEVEL_FATAL, "fault", error_message.c_str());
    sentry_set_tag("signal", strsignal(sig));
    sentry_set_tag("server_name", getFQDNOrHostName().c_str());
    sentry_set_extra("signal_number", sentry_value_new_int32(sig));
    setExtras();

    sentry_value_t frames = sentry_value_new_list();

    size_t stack_size = stack_trace.getSize();
    if (stack_size > 0)
    {
        size_t offset = stack_trace.getOffset();
        if (stack_size == 1)
        {
            offset = 1;
        }
        char instruction_addr[100];
        for (size_t i = stack_size - 1; i >= offset; --i)
        {
            const StackTrace::Frame & current_frame = stack_trace.getFrames().value()[i];
            sentry_value_t frame = sentry_value_new_object();
            unsigned long long frame_ptr = reinterpret_cast<unsigned long long>(current_frame.virtual_addr);
            snprintf(instruction_addr, sizeof(instruction_addr), "0x%llx", frame_ptr);
            sentry_value_set_by_key(frame, "instruction_addr", sentry_value_new_string(instruction_addr));

            if (current_frame.symbol.has_value())
            {
                sentry_value_set_by_key(frame, "function", sentry_value_new_string(current_frame.symbol.value().c_str()));
            }

            if (current_frame.file.has_value())
            {
                sentry_value_set_by_key(frame, "filename", sentry_value_new_string(current_frame.file.value().c_str()));
            }

            if (current_frame.line.has_value())
            {
                sentry_value_set_by_key(frame, "lineno", sentry_value_new_int32(current_frame.line.value()));
            }

            sentry_value_append(frames, frame);
        }
    }

    sentry_value_t stacktrace = sentry_value_new_object();
    sentry_value_set_by_key(stacktrace, "frames", frames);

    sentry_value_t thread = sentry_value_new_object();
    sentry_value_set_by_key(thread, "stacktrace", stacktrace);

    sentry_value_t values = sentry_value_new_list();
    sentry_value_append(values, thread);

    sentry_value_t threads = sentry_value_new_object();
    sentry_value_set_by_key(threads, "values", values);

    sentry_value_set_by_key(event, "threads", threads);

    sentry_capture_event(event);
    shutdown();
}
