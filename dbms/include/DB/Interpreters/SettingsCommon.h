#pragma once

#include <DB/Core/Field.h>
#include <DB/IO/WriteHelpers.h>
#include <Poco/Timespan.h>
#include <cpuid/libcpuid.h>
#include <DB/IO/CompressedStream.h>

namespace DB
{


/** Одна настройка какого-либо типа.
  * Хранит внутри себя значение, а также флаг - было ли значение изменено.
  * Это сделано, чтобы можно было отправлять на удалённые серверы только изменённые (или явно указанные в конфиге) значения.
  * То есть, если настройка не была указана в конфиге и не была изменена динамически, то она не отправляется на удалённый сервер,
  *  и удалённый сервер будет использовать своё значение по-умолчанию.
  */


struct SettingUInt64
{
	UInt64 value;
	bool changed = false;

	SettingUInt64(UInt64 x = 0) : value(x) {}

	operator UInt64() const { return value; }
	SettingUInt64 & operator= (UInt64 x) { set(x); return *this; }

	String toString() const
	{
		return DB::toString(value);
	}

	void set(UInt64 x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		set(safeGet<UInt64>(x));
	}

	void set(const String & x)
	{
		set(parse<UInt64>(x));
	}

	void set(ReadBuffer & buf)
	{
		UInt64 x = 0;
		readVarUInt(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeVarUInt(value, buf);
	}
};

typedef SettingUInt64 SettingBool;


/** В отличие от SettingUInt64, поддерживает значение 'auto' - количество процессорных ядер без учёта SMT.
  * Значение 0 так же воспринимается как auto.
  * При сериализации, auto записывается так же, как 0.
  */
struct SettingMaxThreads
{
	UInt64 value;
	bool is_auto;
	bool changed = false;

	SettingMaxThreads(UInt64 x = 0) : value(x ? x : getAutoValue()), is_auto(x == 0) {}

	operator UInt64() const { return value; }
	SettingMaxThreads & operator= (UInt64 x) { set(x); return *this; }

	String toString() const
	{
		/// Вместо значения auto выводим актуальное значение, чтобы его было легче посмотреть.
		return DB::toString(value);
	}

	void set(UInt64 x)
	{
		value = x ? x : getAutoValue();
		is_auto = x == 0;
		changed = true;
	}

	void set(const Field & x)
	{
		if (x.getType() == Field::Types::String)
			set(safeGet<const String &>(x));
		else
			set(safeGet<UInt64>(x));
	}

	void set(const String & x)
	{
		if (x == "auto")
			setAuto();
		else
			set(parse<UInt64>(x));
	}

	void set(ReadBuffer & buf)
	{
		UInt64 x = 0;
		readVarUInt(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeVarUInt(is_auto ? 0 : value, buf);
	}

	void setAuto()
	{
		value = getAutoValue();
		is_auto = true;
	}

	UInt64 getAutoValue() const
	{
		static auto res = getAutoValueImpl();
		return res;
	}

	/// Выполняется один раз за всё время. Выполняется из одного потока.
	UInt64 getAutoValueImpl() const
	{
		cpu_raw_data_t raw_data;
		if (0 != cpuid_get_raw_data(&raw_data))
			throw Exception("Cannot cpuid_get_raw_data: " + String(cpuid_error()), ErrorCodes::CPUID_ERROR);

		cpu_id_t data;
		if (0 != cpu_identify(&raw_data, &data))
			throw Exception("Cannot cpu_identify: " + String(cpuid_error()), ErrorCodes::CPUID_ERROR);

		return data.num_cores * data.total_logical_cpus / data.num_logical_cpus;
	}
};


struct SettingSeconds
{
	Poco::Timespan value;
	bool changed = false;

	SettingSeconds(UInt64 seconds = 0) : value(seconds, 0) {}

	operator Poco::Timespan() const { return value; }
	SettingSeconds & operator= (Poco::Timespan x) { set(x); return *this; }

	Poco::Timespan::TimeDiff totalSeconds() const { return value.totalSeconds(); }

	String toString() const
	{
		return DB::toString(totalSeconds());
	}

	void set(Poco::Timespan x)
	{
		value = x;
		changed = true;
	}

	void set(UInt64 x)
	{
		set(Poco::Timespan(x, 0));
	}

	void set(const Field & x)
	{
		set(safeGet<UInt64>(x));
	}

	void set(const String & x)
	{
		set(parse<UInt64>(x));
	}

	void set(ReadBuffer & buf)
	{
		UInt64 x = 0;
		readVarUInt(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeVarUInt(value.totalSeconds(), buf);
	}
};


struct SettingMilliseconds
{
	Poco::Timespan value;
	bool changed = false;

	SettingMilliseconds(UInt64 milliseconds = 0) : value(milliseconds * 1000) {}

	operator Poco::Timespan() const { return value; }
	SettingMilliseconds & operator= (Poco::Timespan x) { set(x); return *this; }

	Poco::Timespan::TimeDiff totalMilliseconds() const { return value.totalMilliseconds(); }

	String toString() const
	{
		return DB::toString(totalMilliseconds());
	}

	void set(Poco::Timespan x)
	{
		value = x;
		changed = true;
	}

	void set(UInt64 x)
	{
		set(Poco::Timespan(x * 1000));
	}

	void set(const Field & x)
	{
		set(safeGet<UInt64>(x));
	}

	void set(const String & x)
	{
		set(parse<UInt64>(x));
	}

	void set(ReadBuffer & buf)
	{
		UInt64 x = 0;
		readVarUInt(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeVarUInt(value.totalMilliseconds(), buf);
	}
};


struct SettingFloat
{
	float value;
	bool changed = false;

	SettingFloat(float x = 0) : value(x) {}

	operator float() const { return value; }
	SettingFloat & operator= (float x) { set(x); return *this; }

	String toString() const
	{
		return DB::toString(value);
	}

	void set(float x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		if (x.getType() == Field::Types::UInt64)
		{
			set(safeGet<UInt64>(x));
		}
		else if (x.getType() == Field::Types::Int64)
		{
			set(safeGet<Int64>(x));
		}
		else if (x.getType() == Field::Types::Float64)
		{
			set(safeGet<Float64>(x));
		}
		else
			throw Exception(std::string("Bad type of setting. Expected UInt64, Int64 or Float64, got ") + x.getTypeName(), ErrorCodes::TYPE_MISMATCH);
	}

	void set(const String & x)
	{
		set(parse<float>(x));
	}

	void set(ReadBuffer & buf)
	{
		String x;
		readBinary(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeBinary(toString(), buf);
	}
};


enum class LoadBalancing
{
	/// среди реплик с минимальным количеством ошибок выбирается случайная
	RANDOM = 0,
	/// среди реплик с минимальным количеством ошибок выбирается реплика
	/// с минимальным количеством отличающихся символов в имени реплики и имени локального хоста
	NEAREST_HOSTNAME,
	/// реплики перебираются строго по порядку; количество ошибок не имеет значение
	IN_ORDER,
};

struct SettingLoadBalancing
{
	LoadBalancing value;
	bool changed = false;

	SettingLoadBalancing(LoadBalancing x) : value(x) {}

	operator LoadBalancing() const { return value; }
	SettingLoadBalancing & operator= (LoadBalancing x) { set(x); return *this; }

	static LoadBalancing getLoadBalancing(const String & s)
	{
		if (s == "random") 				return LoadBalancing::RANDOM;
		if (s == "nearest_hostname") 	return LoadBalancing::NEAREST_HOSTNAME;
		if (s == "in_order") 			return LoadBalancing::IN_ORDER;

		throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
			ErrorCodes::UNKNOWN_LOAD_BALANCING);
	}

	String toString() const
	{
		const char * strings[] = {"random", "nearest_hostname", "in_order"};
		if (value < LoadBalancing::RANDOM || value > LoadBalancing::IN_ORDER)
			throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
		return strings[static_cast<size_t>(value)];
	}

	void set(LoadBalancing x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		set(safeGet<const String &>(x));
	}

	void set(const String & x)
	{
		set(getLoadBalancing(x));
	}

	void set(ReadBuffer & buf)
	{
		String x;
		readBinary(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeBinary(toString(), buf);
	}
};


/// Какие строки включать в TOTALS.
enum class TotalsMode
{
	BEFORE_HAVING			= 0, /// Считать HAVING по всем прочитанным строкам;
								 ///  включая не попавшие в max_rows_to_group_by
								 ///  и не прошедшие HAVING после группировки.
	AFTER_HAVING_INCLUSIVE	= 1, /// Считать по всем строкам, кроме не прошедших HAVING;
								 ///  то есть, включать в TOTALS все строки, не прошедшие max_rows_to_group_by.
	AFTER_HAVING_EXCLUSIVE	= 2, /// Включать только строки, прошедшие и max_rows_to_group_by, и HAVING.
	AFTER_HAVING_AUTO		= 3, /// Автоматически выбирать между INCLUSIVE и EXCLUSIVE,
};

struct SettingTotalsMode
{
	TotalsMode value;
	bool changed = false;

	SettingTotalsMode(TotalsMode x) : value(x) {}

	operator TotalsMode() const { return value; }
	SettingTotalsMode & operator= (TotalsMode x) { set(x); return *this; }

	static TotalsMode getTotalsMode(const String & s)
	{
		if (s == "before_having") 			return TotalsMode::BEFORE_HAVING;
		if (s == "after_having_exclusive")	return TotalsMode::AFTER_HAVING_EXCLUSIVE;
		if (s == "after_having_inclusive")	return TotalsMode::AFTER_HAVING_INCLUSIVE;
		if (s == "after_having_auto")		return TotalsMode::AFTER_HAVING_AUTO;

		throw Exception("Unknown totals mode: '" + s + "', must be one of 'before_having', 'after_having_exclusive', 'after_having_inclusive', 'after_having_auto'", ErrorCodes::UNKNOWN_TOTALS_MODE);
	}

	String toString() const
	{
		switch (value)
		{
			case TotalsMode::BEFORE_HAVING:				return "before_having";
			case TotalsMode::AFTER_HAVING_EXCLUSIVE:	return "after_having_exclusive";
			case TotalsMode::AFTER_HAVING_INCLUSIVE:	return "after_having_inclusive";
			case TotalsMode::AFTER_HAVING_AUTO:			return "after_having_auto";

			default:
				throw Exception("Unknown TotalsMode enum value", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
		}
	}

	void set(TotalsMode x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		set(safeGet<const String &>(x));
	}

	void set(const String & x)
	{
		set(getTotalsMode(x));
	}

	void set(ReadBuffer & buf)
	{
		String x;
		readBinary(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeBinary(toString(), buf);
	}
};

/// Что делать, если ограничение превышено.
enum class OverflowMode
{
	THROW 	= 0,	/// Кинуть исключение.
	BREAK 	= 1,	/// Прервать выполнение запроса, вернуть что есть.
	ANY		= 2,	/** Только для GROUP BY: не добавлять новые строки в набор,
						* но продолжать агрегировать для ключей, успевших попасть в набор.
						*/
};

template <bool enable_mode_any>
struct SettingOverflowMode
{
	OverflowMode value;
	bool changed = false;

	SettingOverflowMode(OverflowMode x = OverflowMode::THROW) : value(x) {}

	operator OverflowMode() const { return value; }
	SettingOverflowMode & operator= (OverflowMode x) { set(x); return *this; }

	static OverflowMode getOverflowModeForGroupBy(const String & s)
	{
		if (s == "throw") 	return OverflowMode::THROW;
		if (s == "break") 	return OverflowMode::BREAK;
		if (s == "any")		return OverflowMode::ANY;

		throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
	}

	static OverflowMode getOverflowMode(const String & s)
	{
		OverflowMode mode = getOverflowModeForGroupBy(s);

		if (mode == OverflowMode::ANY && !enable_mode_any)
			throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

		return mode;
	}

	String toString() const
	{
		const char * strings[] = { "throw", "break", "any" };

		if (value < OverflowMode::THROW || value > OverflowMode::ANY)
			throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

		return strings[static_cast<size_t>(value)];
	}

	void set(OverflowMode x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		set(safeGet<const String &>(x));
	}

	void set(const String & x)
	{
		set(getOverflowMode(x));
	}

	void set(ReadBuffer & buf)
	{
		String x;
		readBinary(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeBinary(toString(), buf);
	}
};

struct SettingCompressionMethod
{
	CompressionMethod value;
	bool changed = false;

	SettingCompressionMethod(CompressionMethod x = CompressionMethod::LZ4) : value(x) {}

	operator CompressionMethod() const { return value; }
	SettingCompressionMethod & operator= (CompressionMethod x) { set(x); return *this; }

	static CompressionMethod getCompressionMethod(const String & s)
	{
		if (s == "quicklz")
			return CompressionMethod::QuickLZ;
		if (s == "lz4")
			return CompressionMethod::LZ4;
		if (s == "lz4hc")
			return CompressionMethod::LZ4HC;
		if (s == "zstd")
			return CompressionMethod::ZSTD;

		throw Exception("Unknown compression method: '" + s + "', must be one of 'quicklz', 'lz4', 'lz4hc', 'zstd' ", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
	}

	String toString() const
	{
		const char * strings[] = { "quicklz", "lz4", "lz4hc", "zstd" };

		if (value < CompressionMethod::QuickLZ || value > CompressionMethod::ZSTD)
			throw Exception("Unknown compression method", ErrorCodes::UNKNOWN_COMPRESSION_METHOD);

		return strings[static_cast<size_t>(value)];
	}

	void set(CompressionMethod x)
	{
		value = x;
		changed = true;
	}

	void set(const Field & x)
	{
		set(safeGet<const String &>(x));
	}

	void set(const String & x)
	{
		set(getCompressionMethod(x));
	}

	void set(ReadBuffer & buf)
	{
		String x;
		readBinary(x, buf);
		set(x);
	}

	void write(WriteBuffer & buf) const
	{
		writeBinary(toString(), buf);
	}
};
}
