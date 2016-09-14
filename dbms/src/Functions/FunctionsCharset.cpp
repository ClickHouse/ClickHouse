#include <DB/Functions/IFunction.h>
#include <DB/Functions/ObjectPool.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <ext/range.hpp>

#include <iconv.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int BAD_ARGUMENTS;
	extern const int LOGICAL_ERROR;
	extern const int CANNOT_ICONV;
}


/** convertCharset(s, from, to)
  *
  * Assuming string 's' contains bytes in charset 'from',
  *  returns another string with bytes, representing same content in charset 'to'.
  * from and to must be constants.
  *
  * When bytes are illegal in 'from' charset or are not representable in 'to' charset,
  *  behavior is implementation specific.
  */
class FunctionConvertCharset : public IFunction
{
private:
	using CharsetsFromTo = std::pair<String, String>;

	struct IConv
	{
		iconv_t impl;

		IConv(const CharsetsFromTo & charsets)
		{
			impl = iconv_open(charsets.second.data(), charsets.first.data());
			if (impl == reinterpret_cast<iconv_t>(-1))
				throwFromErrno("Cannot iconv_open with charsets " + charsets.first + " and " + charsets.second,
					ErrorCodes::BAD_ARGUMENTS);
		}

		~IConv()
		{
			if (-1 == iconv_close(impl))	/// throwing exception leads to std::terminate and it's ok.
				throwFromErrno("Cannot iconv_close",
					ErrorCodes::LOGICAL_ERROR);
		}
	};

	/// Separate converter is created for each thread.
	using Pool = ObjectPool<IConv, CharsetsFromTo>;

	Pool::Pointer getConverter(const CharsetsFromTo & charsets)
	{
		static Pool pool;
		return pool.get(charsets, [&charsets] { return new IConv(charsets); });
	}

	void convert(const String & from_charset, const String & to_charset,
		const ColumnString::Chars_t & from_chars, const ColumnString::Offsets_t & from_offsets,
		ColumnString::Chars_t & to_chars, ColumnString::Offsets_t & to_offsets)
	{
		auto converter = getConverter(CharsetsFromTo(from_charset, to_charset));
		iconv_t iconv_state = converter->impl;

		to_chars.resize(from_chars.size());
		to_offsets.resize(from_offsets.size());

		ColumnString::Offset_t current_from_offset = 0;
		ColumnString::Offset_t current_to_offset = 0;

		size_t size = from_offsets.size();

		for (size_t i = 0; i < size; ++i)
		{
			size_t from_string_size = from_offsets[i] - current_from_offset - 1;

			/// We assume that empty string is empty in every charset.
			if (0 != from_string_size)
			{
				/// reset state of iconv
				size_t res = iconv(iconv_state, nullptr, nullptr, nullptr, nullptr);
				if (static_cast<size_t>(-1) == res)
					throwFromErrno("Cannot reset iconv", ErrorCodes::CANNOT_ICONV);

				/// perform conversion; resize output buffer and continue if required

				char * in_buf = const_cast<char *>(reinterpret_cast<const char *>(&from_chars[current_from_offset]));
				size_t in_bytes_left = from_string_size;

				char * out_buf = reinterpret_cast<char *>(&to_chars[current_to_offset]);
				size_t out_bytes_left = to_chars.size() - current_to_offset;

				while (in_bytes_left)
				{
					size_t res = iconv(iconv_state, &in_buf, &in_bytes_left, &out_buf, &out_bytes_left);
					current_to_offset = to_chars.size() - out_bytes_left;

					if (static_cast<size_t>(-1) == res)
					{
						if (E2BIG == errno)
						{
							to_chars.resize(to_chars.size() * 2);
							out_buf = reinterpret_cast<char *>(&to_chars[current_to_offset]);
							out_bytes_left = to_chars.size() - current_to_offset;
							continue;
						}

						throwFromErrno("Cannot convert charset", ErrorCodes::CANNOT_ICONV);
					}
				}
			}

			if (to_chars.size() < current_to_offset + 1)
				to_chars.resize(current_to_offset + 1);

			to_chars[current_to_offset] = 0;

			++current_to_offset;
			to_offsets[i] = current_to_offset;

			current_from_offset = from_offsets[i];
		}

		to_chars.resize(current_to_offset);
	}

public:
	static constexpr auto name = "convertCharset";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionConvertCharset>(); }

	String getName() const override
	{
		return name;
	}

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		for (size_t i : ext::range(0, 3))
			if (!typeid_cast<const DataTypeString *>(&*arguments[i]))
				throw Exception("Illegal type " + arguments[i]->getName() + " of argument of function " + getName()
					+ ", must be String", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnWithTypeAndName & arg_from = block.unsafeGetByPosition(arguments[0]);
		const ColumnWithTypeAndName & arg_charset_from = block.unsafeGetByPosition(arguments[1]);
		const ColumnWithTypeAndName & arg_charset_to = block.unsafeGetByPosition(arguments[2]);
		ColumnWithTypeAndName & res = block.unsafeGetByPosition(result);

		const ColumnConstString * col_charset_from = typeid_cast<const ColumnConstString *>(arg_charset_from.column.get());
		const ColumnConstString * col_charset_to = typeid_cast<const ColumnConstString *>(arg_charset_to.column.get());

		if (!col_charset_from || !col_charset_to)
			throw Exception("2nd and 3rd arguments of function " + getName() + " (source charset and destination charset) must be constant strings.",
				ErrorCodes::ILLEGAL_COLUMN);

		String charset_from = col_charset_from->getData();
		String charset_to = col_charset_to->getData();

		if (const ColumnString * col_from = typeid_cast<const ColumnString *>(arg_from.column.get()))
		{
			auto col_to = std::make_shared<ColumnString>();
			convert(charset_from, charset_to, col_from->getChars(), col_from->getOffsets(), col_to->getChars(), col_to->getOffsets());
			res.column = col_to;
		}
		else if (const ColumnConstString * col_from = typeid_cast<const ColumnConstString *>(arg_from.column.get()))
		{
			auto full_column_holder = col_from->cloneResized(1)->convertToFullColumnIfConst();
			const ColumnString * col_from_full = static_cast<const ColumnString *>(full_column_holder.get());

			auto col_to_full = std::make_shared<ColumnString>();
			convert(charset_from, charset_to, col_from_full->getChars(), col_from_full->getOffsets(), col_to_full->getChars(), col_to_full->getOffsets());

			res.column = std::make_shared<ColumnConstString>(col_from->size(), (*col_to_full)[0].get<String>(), res.type);
		}
		else
			throw Exception("Illegal column passed as first argument of function " + getName() + " (must be ColumnString).",
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


void registerFunctionsCharset(FunctionFactory & factory)
{
	factory.registerFunction<FunctionConvertCharset>();
}

}
