#pragma once

#include <Common/config.h>

#if USE_AEROSPIKE

#include <sstream>

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_val.h>

namespace DB
{

using AerospikeKey = std::unique_ptr<as_key>;

class Aerospike
{
public:
    using Keys = std::vector<AerospikeKey>;

    class Scan
    {
    public:
        Scan(const char * space, const char * set)
        {
            as_scan_init(&scanner, space, set);
        }

        ~Scan()
        {
            as_scan_destroy(&scanner);
        }

        bool scanForeach(Aerospike & as, aerospike_scan_foreach_callback callback, void * udata)
        {
            const as_policy_scan * policy = nullptr;
            return AEROSPIKE_OK == aerospike_scan_foreach(as.getClient(), &as.error, policy, &scanner, callback, udata);
        }

    private:
        as_scan scanner;
    };

    Aerospike(const char * host, UInt16 port)
    {
        as_config config;
        as_config_init(&config);
        as_config_add_host(&config, host, port);
        aerospike_init(&client, &config);
        connect();
    }

    ~Aerospike()
    {
        disconnect();
        aerospike_destroy(&client);
    }

    aerospike * getClient() { return &client; }
    const aerospike * getClient() const { return &client; }

    as_error connect()
    {
        aerospike_connect(&client, &error);
        return error;
    }

    as_error disconnect()
    {
        aerospike_close(&client, &error);
        return error;
    }

    bool isConnected() { return aerospike_cluster_is_connected(&client); }

    bool batchGet(const as_batch * batch, aerospike_batch_read_callback callback, void * udata)
    {
        const as_policy_batch * policy = nullptr;
        return AEROSPIKE_OK == aerospike_batch_get(&client, &error, policy, batch, callback, udata);
    }

    std::string lastErrorMessage() const
    {
        std::stringstream ss;
        ss << "Aerospike error(" << error.code << ")";
        if (error.message)
            ss << " " << error.message;
        if (error.file && error.line)
            ss <<" at [" << error.file << ":" << error.line << "]";
        return ss.str();
    }

private:
    aerospike client;
    as_error error;
};

inline AerospikeKey AllocateKey(const as_key & key, const char * namespace_name, const char * set_name)
{
    AerospikeKey result;
    switch (as_val_type(const_cast<as_key_value*>(&key.value)))
    {
        case AS_INTEGER:
        {
            return AerospikeKey(as_key_new_int64(namespace_name, set_name, key.value.integer.value));
        }
        case AS_STRING:
        {
            size_t value_size = key.value.string.len;
            char * v = static_cast<char *>(cf_malloc(value_size + 1));
            memcpy(v, key.value.string.value, value_size);
            v[value_size] = 0;
            as_key * current_key = as_key_new_str(namespace_name, set_name, v);
            current_key->value.string.len = value_size;
            return AerospikeKey(current_key);
        }
        default:
        {
            size_t value_size = key.value.string.len;
            void * v = cf_malloc(value_size);
            memcpy(v, key.value.bytes.value, value_size);
            return AerospikeKey(as_key_new_rawp(namespace_name, set_name, static_cast<uint8_t *>(v), value_size, true));
        }
    }
}

inline void InitializeBatchKey(as_key * new_key, const char * namespace_name, const char * set_name, const AerospikeKey & base_key)
{
    switch (as_val_type(&base_key->value))
    {
        case AS_INTEGER:
            as_key_init_int64(new_key, namespace_name, set_name, base_key->value.integer.value);
            break;
        case AS_STRING:
            as_key_init_str(new_key, namespace_name, set_name, base_key->value.string.value);
            break;
        default:
            const as_bytes & bytes = base_key->value.bytes;
            as_key_init_raw(new_key, namespace_name, set_name, bytes.value, bytes.size);
            break;
    }
}

}

#endif
