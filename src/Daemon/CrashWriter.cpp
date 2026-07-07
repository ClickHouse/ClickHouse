#include <Daemon/CrashWriter.h>

#include <Poco/Util/Application.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Poco/Environment.h>

#include <base/getMemoryAmount.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>
#include <Common/SymbolIndex.h>
#include <Common/StackTrace.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Core/ServerUUID.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromHTTP.h>

#include <Common/config_version.h>

using namespace DB;


std::unique_ptr<CrashWriter> CrashWriter::instance;

void CrashWriter::initialize(Poco::Util::LayeredConfiguration & config)
{
    instance.reset(new CrashWriter(config));
}

bool CrashWriter::initialized()
{
    return instance != nullptr;
}

CrashWriter::CrashWriter(Poco::Util::LayeredConfiguration & config)
{
    auto logger = getLogger("CrashWriter");

    if (config.getBool("send_crash_reports.enabled", false))
    {
        endpoint = config.getString("send_crash_reports.endpoint");
        server_data_path = config.getString("path", "");
        LOG_DEBUG(logger, "Sending crash reports is initialized with {} endpoint (anonymized)", endpoint);
    }
    else
    {
        LOG_DEBUG(logger, "Sending crash reports is disabled");
    }
}

void CrashWriter::onSignal(int sig, std::string_view error_message, const FramePointers & frame_pointers, size_t offset, size_t size)
{
    if (instance)
        instance->sendError(Type::SIGNAL, sig, error_message, frame_pointers, offset, size);
}

void CrashWriter::onException(int code, std::string_view format_string, const FramePointers & frame_pointers, size_t offset, size_t size)
{
    if (instance)
        instance->sendError(Type::EXCEPTION, code, format_string, frame_pointers, offset, size);
}

void CrashWriter::sendError(Type type, int sig_or_error, std::string_view error_message, const FramePointers & frame_pointers, size_t offset, size_t size)
{
    auto logger = getLogger("CrashWriter");
    if (!instance)
    {
        LOG_INFO(logger, "Not sending crash report");
        return;
    }

    try
    {
        LOG_INFO(logger, "Sending crash report");

        auto out = BuilderWriteBufferFromHTTP(Poco::URI{endpoint})
            .withBufferSize(4096).withConnectionGroup(HTTPConnectionGroupType::HTTP).create();
        auto & json = *out;

        FormatSettings settings;

        writeChar('{', json);
        writeCString("\"message\":", json);
        writeJSONString(error_message, json, settings);

        switch (type)
        {
            case SIGNAL:
            {
                writeCString(",\"type\":\"signal\"", json);
                writeCString(",\"signal_number\":", json);
                writeIntText(sig_or_error, json);
                break;
            }
            case EXCEPTION:
            {
                writeCString(",\"type\":\"exception\"", json);
                writeCString(",\"exception_code\":", json);
                writeIntText(sig_or_error, json);
                writeCString(",\"exception_message\":", json);
                writeJSONString(ErrorCodes::getName(sig_or_error), json, settings);
                break;
            }
        }

        #if defined(__ELF__) && !defined(OS_FREEBSD)
            const String & build_id_hex = SymbolIndex::instance().getBuildIDHex();
            writeCString(",\"build_id\":", json);
            writeJSONString(build_id_hex, json, settings);
        #endif

        UUID server_uuid = ServerUUID::get();
        if (server_uuid != UUIDHelpers::Nil)
        {
            std::string server_uuid_str = toString(server_uuid);
            writeCString(",\"server_uuid\":", json);
            writeJSONString(server_uuid_str, json, settings);
        }

        writeCString(",\"version\":", json);
        writeJSONString(VERSION_STRING, json, settings);
        writeCString(",\"git_hash\":", json);
        writeJSONString(VERSION_GITHASH, json, settings);
        writeCString(",\"git_version\":", json);
        writeJSONString(VERSION_DESCRIBE, json, settings);
        writeCString(",\"revision\":", json);
        writeIntText(VERSION_REVISION, json);
        writeCString(",\"official\":", json);
        writeJSONString(VERSION_OFFICIAL, json, settings);

        writeCString(",\"os\":", json);
        writeJSONString(Poco::Environment::osName(), json, settings);
        writeCString(",\"os_version\":", json);
        writeJSONString(Poco::Environment::osVersion(), json, settings);
        writeCString(",\"architecture\":", json);
        writeJSONString(Poco::Environment::osArchitecture(), json, settings);

        writeCString(",\"total_ram\":", json);
        writeIntText(getMemoryAmountOrZero(), json);
        writeCString(",\"cpu_cores\":", json);
        writeIntText(getNumberOfCPUCoresToUse(), json);

        if (!server_data_path.empty())
        {
            writeCString(",\"disk_free_space\":", json);
            writeIntText(std::filesystem::space(server_data_path).free, json);
        }

        /// Prepare data for the stack trace.
        if (size > 0)
        {
            char instruction_addr[19]
            {
                '0', 'x',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
                '\0'
            };

            bool first = true;
            auto add_frame = [&](const StackTrace::Frame & current_frame)
            {
                UInt64 frame_ptr = reinterpret_cast<UInt64>(current_frame.physical_addr);
                writeHexUIntLowercase(frame_ptr, instruction_addr + 2);

                if (!first)
                    writeChar(',', json);
                first = false;

                writeCString("{\"addr\":", json);
                writeJSONString(instruction_addr, json, settings);
                if (current_frame.symbol)
                {
                    writeCString(",\"function\":", json);
                    writeJSONString(current_frame.symbol.value(), json, settings);
                }
                if (current_frame.file)
                {
                    writeCString(",\"file\":", json);
                    writeJSONString(current_frame.file.value(), json, settings);
                }
                if (current_frame.line)
                {
                    writeCString(",\"line\":", json);
                    writeIntText(current_frame.line.value(), json);
                }
                writeChar('}', json);
            };

            writeCString(",\"trace\":[", json);
            StackTrace::forEachFrame(frame_pointers, offset, size, add_frame, /* fatal= */ true);
            writeChar(']', json);
        }

        writeChar('}', json);

        json.next();
        json.finalize();
    }
    catch (...)
    {
        LOG_INFO(logger, "Cannot send a crash report: {}", getCurrentExceptionMessage(__PRETTY_FUNCTION__));
    }
}
