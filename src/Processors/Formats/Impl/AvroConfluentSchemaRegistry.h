#pragma once

#include "config.h"

#if USE_AVRO

#include <memory>
#include <string>

#include <Common/CacheBase.h>
#include <Common/CurrentMetrics.h>

#include <Formats/FormatSettings.h>

#include <ValidSchema.hh>

#include <Poco/URI.h>
#include <Poco/Net/HTTPRequest.h>


namespace DB
{

/// Client for the Confluent Schema Registry.
/// Supports fetching schemas by ID and registering new schemas under a subject.
/// Thread-safe: the internal schema cache uses locking, and HTTP calls are stateless.
class ConfluentSchemaRegistry
{
public:
    explicit ConfluentSchemaRegistry(const std::string & base_url_, size_t schema_cache_max_size = 1000);

    /// Fetch a schema by ID (GET /schemas/ids/{id}). Results are cached.
    avro::ValidSchema getSchema(uint32_t id, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts);

    /// Register a schema under a subject (POST /subjects/{subject}/versions).
    /// Returns the global schema ID. The call is idempotent: if the schema
    /// already exists under the subject, the existing ID is returned.
    uint32_t registerSchema(const std::string & subject, const avro::ValidSchema & schema, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts);

private:
    avro::ValidSchema fetchSchema(uint32_t id, const FormatSettings::AvroSchemaRegistryTimeouts & timeouts);

    /// Apply HTTP Basic Auth credentials extracted from the base URL.
    void applyAuth(const Poco::URI & url, Poco::Net::HTTPRequest & request) const;

    /// Cached schema registration. The full `schema_json` is kept alongside the
    /// id so we can compare it on every lookup and detect hash collisions on
    /// the cache key.
    struct CachedSchemaRegistration
    {
        std::string schema_json;
        uint32_t schema_id;
    };

    Poco::URI base_url;
    CacheBase<uint32_t, avro::ValidSchema> schema_cache;
    /// Cache keyed on `subject + hash(schema_json)`, populated on successful
    /// registration. The cached value carries the full schema string so we can
    /// verify equality on hit before trusting the id.
    CacheBase<std::string, CachedSchemaRegistration> register_cache;
};

/// Global cache of ConfluentSchemaRegistry instances, keyed by base URL.
/// Ensures that multiple format instances talking to the same registry share
/// the schema cache and HTTP connection pool.
std::shared_ptr<ConfluentSchemaRegistry> getConfluentSchemaRegistry(const FormatSettings & format_settings);

/// Drop the global registry-instance cache. Forces subsequent calls to construct
/// fresh `ConfluentSchemaRegistry` objects, which discards their per-URL fetch and
/// register caches. Backs `SYSTEM DROP AVRO SCHEMA CACHE`.
void clearConfluentSchemaRegistryCache();

}

#endif
