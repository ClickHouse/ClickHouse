#include <DB/IO/ReadBuffer.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Interpreters/Settings.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/Util/Application.h>


namespace DB
{

static LoadBalancing::LoadBalancing getLoadBalancing(const String & s)
{
	if (s == "random") 	return LoadBalancing::RANDOM;
	if (s == "nearest_hostname") 	return LoadBalancing::NEAREST_HOSTNAME;

	throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname'", ErrorCodes::UNKNOWN_LOAD_BALANCING);
}

void Settings::set(const String & name, const Field & value)
{
		 if (name == "max_block_size")		max_block_size 		= safeGet<UInt64>(value);
	else if (name == "max_threads")			max_threads 		= safeGet<UInt64>(value);
	else if (name == "max_query_size")		max_query_size 		= safeGet<UInt64>(value);
	else if (name == "asynchronous")		asynchronous 		= safeGet<UInt64>(value);
	else if (name == "interactive_delay") 	interactive_delay 	= safeGet<UInt64>(value);
	else if (name == "connect_timeout")		connect_timeout 	= Poco::Timespan(safeGet<UInt64>(value), 0);
	else if (name == "receive_timeout")		receive_timeout 	= Poco::Timespan(safeGet<UInt64>(value), 0);
	else if (name == "send_timeout")		send_timeout 		= Poco::Timespan(safeGet<UInt64>(value), 0);
	else if (name == "queue_max_wait_ms")	queue_max_wait_ms 	= Poco::Timespan(safeGet<UInt64>(value) * 1000);
	else if (name == "poll_interval")		poll_interval 		= safeGet<UInt64>(value);
	else if (name == "connect_timeout_with_failover_ms")
		connect_timeout_with_failover_ms = Poco::Timespan(safeGet<UInt64>(value) * 1000);
	else if (name == "max_distributed_connections") max_distributed_connections = safeGet<UInt64>(value);
	else if (name == "distributed_connections_pool_size") distributed_connections_pool_size = safeGet<UInt64>(value);
	else if (name == "connections_with_failover_max_tries") connections_with_failover_max_tries = safeGet<UInt64>(value);
	else if (name == "sign_rewrite")		sign_rewrite 		= safeGet<UInt64>(value);
	else if (name == "extremes")			extremes 			= safeGet<UInt64>(value);
	else if (name == "use_uncompressed_cache") use_uncompressed_cache = safeGet<UInt64>(value);
	else if (name == "use_splitting_aggregator") use_splitting_aggregator = safeGet<UInt64>(value);
	else if (name == "load_balancing")		load_balancing		= getLoadBalancing(safeGet<const String &>(value));
	else if (name == "default_sample")
	{
		if (value.getType() == Field::Types::UInt64)
		{
			default_sample = safeGet<UInt64>(value);
		}
		else if (value.getType() == Field::Types::Float64)
		{
			default_sample 	= safeGet<Float64>(value);
		}
		else
			throw DB::Exception(std::string("Bad type of setting default_sample. Expected UInt64 or Float64, got ") + value.getTypeName(), ErrorCodes::TYPE_MISMATCH);
	}
	else if (!limits.trySet(name, value))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
}

void Settings::set(const String & name, ReadBuffer & buf)
{
	if (   name == "max_block_size"
		|| name == "max_threads"
		|| name == "max_query_size"
		|| name == "asynchronous"
		|| name == "interactive_delay"
		|| name == "connect_timeout"
		|| name == "receive_timeout"
		|| name == "send_timeout"
		|| name == "queue_max_wait_ms"
		|| name == "poll_interval"
		|| name == "connect_timeout_with_failover_ms"
		|| name == "max_distributed_connections"
		|| name == "distributed_connections_pool_size"
		|| name == "connections_with_failover_max_tries"
		|| name == "sign_rewrite"
		|| name == "extremes"
		|| name == "use_uncompressed_cache"
		|| name == "use_splitting_aggregator")
	{
		UInt64 value = 0;
		readVarUInt(value, buf);
		set(name, value);
	}
	else if (name == "load_balancing")
	{
		String value;
		readBinary(value, buf);
		set(name, value);
	}
	else if (name == "default_sample")
	{
		String value;
		readBinary(value, buf);
		set(name, DB::parse<Float64>(value));
	}
	else if (!limits.trySet(name, buf))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
}

void Settings::set(const String & name, const String & value)
{
	if (   name == "max_block_size"
		|| name == "max_threads"
		|| name == "max_query_size"
		|| name == "asynchronous"
		|| name == "interactive_delay"
		|| name == "connect_timeout"
		|| name == "receive_timeout"
		|| name == "send_timeout"
		|| name == "queue_max_wait_ms"
		|| name == "poll_interval"
		|| name == "connect_timeout_with_failover_ms"
		|| name == "max_distributed_connections"
		|| name == "distributed_connections_pool_size"
		|| name == "connections_with_failover_max_tries"
		|| name == "sign_rewrite"
		|| name == "extremes"
		|| name == "use_uncompressed_cache"
		|| name == "use_splitting_aggregator")
	{
		set(name, parse<UInt64>(value));
	}
	else if (name == "default_sample")
	{
		set(name, parse<Float64>(value));
	}
	else if (name == "load_balancing")
	{
		set(name, Field(value));
	}
	else if (!limits.trySet(name, value))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
}

void Settings::setProfile(const String & profile_name, Poco::Util::AbstractConfiguration & config)
{
	String elem = "profiles." + profile_name;
	
	if (!config.has(elem))
		throw Exception("There is no profile '" + profile_name + "' in configuration file.", ErrorCodes::THERE_IS_NO_PROFILE);

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(elem, config_keys);

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
		set(*it, config.getString(elem + "." + *it));
}

void Settings::deserialize(ReadBuffer & buf)
{
	while (true)
	{
		String name;
		readBinary(name, buf);

		/// Пустая строка - это маркер конца настроек.
		if (name.empty())
			break;

		set(name, buf);
	}
}

void Settings::serialize(WriteBuffer & buf) const
{
	writeStringBinary("max_block_size", buf);						writeVarUInt(max_block_size, buf);
	writeStringBinary("max_threads", buf);							writeVarUInt(max_threads, buf);
	writeStringBinary("max_query_size", buf);						writeVarUInt(max_query_size, buf);
	writeStringBinary("asynchronous", buf);							writeVarUInt(asynchronous, buf);
	writeStringBinary("interactive_delay", buf);					writeVarUInt(interactive_delay, buf);
	writeStringBinary("connect_timeout", buf);						writeVarUInt(connect_timeout.totalSeconds(), buf);
	writeStringBinary("receive_timeout", buf);						writeVarUInt(receive_timeout.totalSeconds(), buf);
	writeStringBinary("send_timeout", buf);							writeVarUInt(send_timeout.totalSeconds(), buf);
	writeStringBinary("queue_max_wait_ms", buf);					writeVarUInt(queue_max_wait_ms.totalMilliseconds(), buf);
	writeStringBinary("poll_interval", buf);						writeVarUInt(poll_interval, buf);
	writeStringBinary("connect_timeout_with_failover_ms", buf);		writeVarUInt(connect_timeout_with_failover_ms.totalMilliseconds(), buf);
	writeStringBinary("max_distributed_connections", buf);			writeVarUInt(max_distributed_connections, buf);
	writeStringBinary("distributed_connections_pool_size", buf);	writeVarUInt(distributed_connections_pool_size, buf);
	writeStringBinary("connections_with_failover_max_tries", buf);	writeVarUInt(connections_with_failover_max_tries, buf);
	writeStringBinary("sign_rewrite", buf);							writeVarUInt(sign_rewrite, buf);
	writeStringBinary("extremes", buf);								writeVarUInt(extremes, buf);
	writeStringBinary("use_uncompressed_cache", buf);				writeVarUInt(use_uncompressed_cache, buf);
	writeStringBinary("use_splitting_aggregator", buf);				writeVarUInt(use_splitting_aggregator, buf);
	writeStringBinary("load_balancing", buf);						writeStringBinary(toString(load_balancing), buf);
	writeStringBinary("default_sample", buf);						writeStringBinary(DB::toString(default_sample), buf);

	limits.serialize(buf);

	/// Пустая строка - это маркер конца настроек.
	writeStringBinary("", buf);
}

std::string Settings::toString(const LoadBalancing::LoadBalancing & load_balancing) const
{
	const char * strings[] =  {"random", "nearest_hostname"};
	if (load_balancing < LoadBalancing::RANDOM || load_balancing > LoadBalancing::NEAREST_HOSTNAME)
		throw Poco::Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
	return strings[load_balancing];
}
}
