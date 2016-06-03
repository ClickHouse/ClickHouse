#include <DB/IO/CompressedStream.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/Common/Exception.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_COMPRESSION_METHOD;
	extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}


/** Позволяет выбрать метод сжатия по указанным в конфигурационном файле условиям.
  * Конфиг выглядит примерно так:

	<compression>

		<!-- Набор вариантов. Варианты проверяются подряд. Побеждает последний сработавший вариант. Если ни один не сработал, то используется lz4. -->
		<case>

			<!-- Условия. Должны сработать одновременно все. Часть условий могут быть не указаны. -->
			<min_part_size>10000000000</min_part_size>		<!-- Минимальный размер куска в байтах. -->
			<min_part_size_ratio>0.01</min_part_size_ratio>	<!-- Минимальный размер куска относительно всех данных таблицы. -->

			<!-- Какой метод сжатия выбрать. -->
			<method>zstd</method>
		</case>

		<case>
				...
		</case>
	</compression>
  */
class CompressionMethodSelector
{
private:
	struct Element
	{
		size_t min_part_size = 0;
		double min_part_size_ratio = 0;
		CompressionMethod method = CompressionMethod::LZ4;

		void setMethod(const std::string & name)
		{
			if (name == "lz4")
				method = CompressionMethod::LZ4;
			else if (name == "zstd")
				method = CompressionMethod::ZSTD;
			else
				throw Exception("Unknown compression method " + name, ErrorCodes::UNKNOWN_COMPRESSION_METHOD);
		}

		Element(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
		{
			min_part_size = parse<size_t>(config.getString(config_prefix + ".min_part_size", "0"));
			min_part_size_ratio = config.getDouble(config_prefix + ".min_part_size_ratio", 0);

			setMethod(config.getString(config_prefix + ".method"));
		}

		bool check(size_t part_size, double part_size_ratio) const
		{
			return part_size >= min_part_size
				&& part_size_ratio >= min_part_size_ratio;
		}
	};

	std::vector<Element> elements;

public:
	CompressionMethodSelector() {}	/// Всегда возвращает метод по-умолчанию.

	CompressionMethodSelector(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
	{
		Poco::Util::AbstractConfiguration::Keys keys;
		config.keys(config_prefix, keys);

		for (const auto & name : keys)
		{
			if (0 != strncmp(name.data(), "case", strlen("case")))
				throw Exception("Unknown element in config: " + config_prefix + "." + name + ", must be 'case'", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

			elements.emplace_back(config, config_prefix + "." + name);
		}
	}

	CompressionMethod choose(size_t part_size, double part_size_ratio) const
	{
		CompressionMethod res = CompressionMethod::LZ4;

		for (const auto & element : elements)
			if (element.check(part_size, part_size_ratio))
				res = element.method;

		return res;
	}
};

}
