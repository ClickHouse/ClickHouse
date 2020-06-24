#include <daemon/SentryWriter.h>

#include <Poco/File.h>
#include <Poco/Util/Application.h>

#include <common/defines.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>
#if !defined(ARCADIA_BUILD)
#    include "Common/config_version.h"
#    include <Common/config.h>
#endif

#if USE_SENTRY
#    include <sentry.h> // Y_IGNORE
#    include <stdio.h>
#    include <filesystem>
#endif


#if USE_SENTRY
namespace
{

bool initialized = false;
bool anonymize = false;

void setExtras()
{

    if (!anonymize)
    {
        sentry_set_extra("server_name", sentry_value_new_string(getFQDNOrHostName().c_str()));
    }
    sentry_set_tag("version", VERSION_STRING);
    sentry_set_extra("version_githash", sentry_value_new_string(VERSION_GITHASH));
    sentry_set_extra("version_describe", sentry_value_new_string(VERSION_DESCRIBE));
    sentry_set_extra("version_integer", sentry_value_new_int32(VERSION_INTEGER));
    sentry_set_extra("version_revision", sentry_value_new_int32(VERSION_REVISION));
    sentry_set_extra("version_major", sentry_value_new_int32(VERSION_MAJOR));
    sentry_set_extra("version_minor", sentry_value_new_int32(VERSION_MINOR));
    sentry_set_extra("version_patch", sentry_value_new_int32(VERSION_PATCH));
}

void sentry_logger(sentry_level_t level, const char * message, va_list args)
{
    auto * logger = &Poco::Logger::get("SentryWriter");
    size_t size = 1024;
    char buffer[size];
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif
    if (vsnprintf(buffer, size, message, args) >= 0)
    {
#ifdef __clang__
#pragma clang diagnostic pop
#endif
        switch (level)
        {
            case SENTRY_LEVEL_DEBUG:
                logger->debug(buffer);
                break;
            case SENTRY_LEVEL_INFO:
                logger->information(buffer);
                break;
            case SENTRY_LEVEL_WARNING:
                logger->warning(buffer);
                break;
            case SENTRY_LEVEL_ERROR:
                logger->error(buffer);
                break;
            case SENTRY_LEVEL_FATAL:
                logger->fatal(buffer);
                break;
        }
    }
}
}
#endif

void SentryWriter::initialize(Poco::Util::LayeredConfiguration & config)
{
#if USE_SENTRY
    bool enabled = false;
    bool debug = config.getBool("send_crash_reports.debug", false);
    auto * logger = &Poco::Logger::get("SentryWriter");
    if (config.getBool("send_crash_reports.enabled", false))
    {
        if (debug || (strlen(VERSION_OFFICIAL) > 0))
        {
            enabled = true;
        }
    }
    if (enabled)
    {
        const std::filesystem::path & default_tmp_path = std::filesystem::path(config.getString("tmp_path", Poco::Path::temp())) / "sentry";
        const std::string & endpoint
            = config.getString("send_crash_reports.endpoint");
        const std::string & temp_folder_path
            = config.getString("send_crash_reports.tmp_path", default_tmp_path);
        Poco::File(temp_folder_path).createDirectories();

        sentry_options_t * options = sentry_options_new();  /// will be freed by sentry_init or sentry_shutdown
        sentry_options_set_release(options, VERSION_STRING_SHORT);
        sentry_options_set_logger(options, &sentry_logger);
        if (debug)
        {
            sentry_options_set_debug(options, 1);
        }
        sentry_options_set_dsn(options, endpoint.c_str());
        sentry_options_set_database_path(options, temp_folder_path.c_str());
        if (strstr(VERSION_DESCRIBE, "-stable") || strstr(VERSION_DESCRIBE, "-lts"))
        {
            sentry_options_set_environment(options, "prod");
        }
        else
        {
            sentry_options_set_environment(options, "test");
        }

        const std::string & http_proxy = config.getString("send_crash_reports.http_proxy", "");
        if (!http_proxy.empty())
        {
            sentry_options_set_http_proxy(options, http_proxy.c_str());
        }

        int init_status = sentry_init(options);
        if (!init_status)
        {
            initialized = true;
            anonymize = config.getBool("send_crash_reports.anonymize", false);
            LOG_INFO(
                logger,
                "Sending crash reports is initialized with {} endpoint and {} temp folder{}",
                endpoint,
                temp_folder_path,
                anonymize ? " (anonymized)" : "");
        }
        else
        {
            LOG_WARNING(logger, "Sending crash reports failed to initialize with {} status", init_status);
        }
    }
    else
    {
        LOG_INFO(logger, "Sending crash reports is disabled");
    }
#else
    UNUSED(config);
#endif
}

void SentryWriter::shutdown()
{
#if USE_SENTRY
    if (initialized)
    {
        sentry_shutdown();
    }
#endif
}

void SentryWriter::onFault(int sig, const siginfo_t & info, const ucontext_t & context, const StackTrace & stack_trace, const String & build_id_hex)
{
#if USE_SENTRY
    auto * logger = &Poco::Logger::get("SentryWriter");
    if (initialized)
    {
        const std::string & error_message = signalToErrorMessage(sig, info, context);
        sentry_value_t event = sentry_value_new_message_event(SENTRY_LEVEL_FATAL, "fault", error_message.c_str());
        sentry_set_tag("signal", strsignal(sig));
        sentry_set_extra("signal_number", sentry_value_new_int32(sig));
        if (!build_id_hex.empty())
        {
            sentry_set_tag("build_id", build_id_hex.c_str());
        }
        setExtras();

        /// Prepare data for https://develop.sentry.dev/sdk/event-payloads/stacktrace/
        sentry_value_t sentry_frames = sentry_value_new_list();
        size_t stack_size = stack_trace.getSize();
        if (stack_size > 0)
        {
            ssize_t offset = stack_trace.getOffset();
            char instruction_addr[100];
            StackTrace::Frames frames;
            StackTrace::symbolize(stack_trace.getFramePointers(), offset, stack_size, frames);
            for (ssize_t i = stack_size - 1; i >= offset; --i)
            {
                const StackTrace::Frame & current_frame = frames[i];
                sentry_value_t sentry_frame = sentry_value_new_object();
                UInt64 frame_ptr = reinterpret_cast<UInt64>(current_frame.virtual_addr);

                if (std::snprintf(instruction_addr, sizeof(instruction_addr), "0x%" PRIx64, frame_ptr) >= 0)
                {
                    sentry_value_set_by_key(sentry_frame, "instruction_addr", sentry_value_new_string(instruction_addr));
                }

                if (current_frame.symbol.has_value())
                {
                    sentry_value_set_by_key(sentry_frame, "function", sentry_value_new_string(current_frame.symbol.value().c_str()));
                }

                if (current_frame.file.has_value())
                {
                    sentry_value_set_by_key(sentry_frame, "filename", sentry_value_new_string(current_frame.file.value().c_str()));
                }

                if (current_frame.line.has_value())
                {
                    sentry_value_set_by_key(sentry_frame, "lineno", sentry_value_new_int32(current_frame.line.value()));
                }

                sentry_value_append(sentry_frames, sentry_frame);
            }
        }

        /// Prepare data for https://develop.sentry.dev/sdk/event-payloads/threads/
        /// Stacktrace is filled only for a single thread that failed
        sentry_value_t stacktrace = sentry_value_new_object();
        sentry_value_set_by_key(stacktrace, "frames", sentry_frames);

        sentry_value_t thread = sentry_value_new_object();
        sentry_value_set_by_key(thread, "stacktrace", stacktrace);

        sentry_value_t values = sentry_value_new_list();
        sentry_value_append(values, thread);

        sentry_value_t threads = sentry_value_new_object();
        sentry_value_set_by_key(threads, "values", values);

        sentry_value_set_by_key(event, "threads", threads);

        LOG_INFO(logger, "Sending crash report");
        sentry_capture_event(event);
        shutdown();
    }
    else
    {
        LOG_INFO(logger, "Not sending crash report");
    }
#else
    UNUSED(sig);
    UNUSED(info);
    UNUSED(context);
    UNUSED(stack_trace);
    UNUSED(build_id_hex);
#endif
}
