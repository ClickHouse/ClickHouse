#include <gtest/gtest.h>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE

#include <memory>
#include <string>

#include <Common/OpenTelemetryTraceContext.h>
#include <Common/RemoteHostFilter.h>
#include <IO/AzureBlobStorage/PocoHTTPClient.h>
#include <IO/S3/tests/TestPocoHTTPServer.h>

#include <azure/core/context.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/url.hpp>

namespace
{

Azure::Core::Http::HttpStatusCode sendAzureRequest(DB::PocoAzureHTTPClient & client, const std::string & url)
{
    Azure::Core::Http::Request request(Azure::Core::Http::HttpMethod::Get, Azure::Core::Url(url));
    auto response = client.Send(request, Azure::Core::Context{});
    if (!response)
        return Azure::Core::Http::HttpStatusCode::None;
    return response->GetStatusCode();
}

}

TEST(IOTestAzureHTTPClient, PropagatesOpenTelemetryTraceContext)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    DB::PocoAzureHTTPClientConfiguration configuration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 0,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {},
    };
    DB::PocoAzureHTTPClient client(configuration);

    DB::OpenTelemetry::TracingContext tracing_context;
    String error;
    ASSERT_TRUE(tracing_context.parseTraceparentHeader("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", error)) << error;
    tracing_context.tracestate = "congo=t61rcWkgMzE";

    {
        DB::OpenTelemetry::TracingContextHolder tracing_context_holder(
            "IOTestAzureHTTPClient::PropagatesOpenTelemetryTraceContext",
            tracing_context,
            std::weak_ptr<DB::OpenTelemetrySpanLog>{});

        const auto expected_traceparent = DB::OpenTelemetry::CurrentContext().composeTraceparentHeader();
        ASSERT_EQ(sendAzureRequest(client, http.getUrl()), Azure::Core::Http::HttpStatusCode::Ok);

        const auto & headers = http.getLastRequestHeader();
        ASSERT_TRUE(headers.has("traceparent"));
        EXPECT_EQ(headers.get("traceparent"), expected_traceparent);
        ASSERT_TRUE(headers.has("tracestate"));
        EXPECT_EQ(headers.get("tracestate"), tracing_context.tracestate);
    }
}

TEST(IOTestAzureHTTPClient, DoesNotPropagateOpenTelemetryTraceContextWithoutCurrentContext)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    DB::PocoAzureHTTPClientConfiguration configuration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 0,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {},
    };
    DB::PocoAzureHTTPClient client(configuration);

    ASSERT_FALSE(DB::OpenTelemetry::CurrentContext().isTraceEnabled());
    ASSERT_EQ(sendAzureRequest(client, http.getUrl()), Azure::Core::Http::HttpStatusCode::Ok);

    const auto & headers = http.getLastRequestHeader();
    EXPECT_FALSE(headers.has("traceparent"));
    EXPECT_FALSE(headers.has("tracestate"));
}

TEST(IOTestAzureHTTPClient, OpenTelemetryTraceContextOverridesExistingTraceHeaders)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    DB::PocoAzureHTTPClientConfiguration configuration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 0,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {
            {"traceparent", "00-00000000000000000000000000000001-0000000000000001-01"},
            {"tracestate", "stale=value"},
        },
    };
    DB::PocoAzureHTTPClient client(configuration);

    DB::OpenTelemetry::TracingContext tracing_context;
    String error;
    ASSERT_TRUE(tracing_context.parseTraceparentHeader("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", error)) << error;

    {
        DB::OpenTelemetry::TracingContextHolder tracing_context_holder(
            "IOTestAzureHTTPClient::OpenTelemetryTraceContextOverridesExistingTraceHeaders",
            tracing_context,
            std::weak_ptr<DB::OpenTelemetrySpanLog>{});

        const auto expected_traceparent = DB::OpenTelemetry::CurrentContext().composeTraceparentHeader();
        ASSERT_EQ(sendAzureRequest(client, http.getUrl()), Azure::Core::Http::HttpStatusCode::Ok);

        const auto & headers = http.getLastRequestHeader();
        ASSERT_TRUE(headers.has("traceparent"));
        EXPECT_EQ(headers.get("traceparent"), expected_traceparent);
        EXPECT_FALSE(headers.has("tracestate"));
    }
}

TEST(IOTestAzureHTTPClient, SkipsInvalidOpenTelemetryTracestate)
{
    TestPocoHTTPServer http;

    DB::RemoteHostFilter remote_host_filter;
    DB::PocoAzureHTTPClientConfiguration configuration{
        .remote_host_filter = remote_host_filter,
        .max_redirects = 0,
        .for_disk_azure = false,
        .request_throttler = {},
        .extra_headers = {
            {"tracestate", "stale=value"},
        },
    };
    DB::PocoAzureHTTPClient client(configuration);

    DB::OpenTelemetry::TracingContext tracing_context;
    String error;
    ASSERT_TRUE(tracing_context.parseTraceparentHeader("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", error)) << error;
    tracing_context.tracestate = "a\nb cd";

    {
        DB::OpenTelemetry::TracingContextHolder tracing_context_holder(
            "IOTestAzureHTTPClient::SkipsInvalidOpenTelemetryTracestate",
            tracing_context,
            std::weak_ptr<DB::OpenTelemetrySpanLog>{});

        const auto expected_traceparent = DB::OpenTelemetry::CurrentContext().composeTraceparentHeader();
        ASSERT_EQ(sendAzureRequest(client, http.getUrl()), Azure::Core::Http::HttpStatusCode::Ok);

        const auto & headers = http.getLastRequestHeader();
        ASSERT_TRUE(headers.has("traceparent"));
        EXPECT_EQ(headers.get("traceparent"), expected_traceparent);
        EXPECT_FALSE(headers.has("tracestate"));
    }
}

#endif
