#include <string>

#include <iostream>
#include <fstream>

#include <Poco/Stopwatch.h>
#include <Poco/SharedPtr.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>

#include <DB/Columns/ColumnNested.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/Columns/ColumnString.h>

#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>

struct Nested {
	DB::PODArray<UInt8> uint8;
	DB::PODArray<UInt64> uint64;
	std::vector<std::string> string;
};

const size_t n = 4;
const size_t sizes[n] = {3, 1, 4, 2};

int main(int argc, char ** argv)
{
	try
	{
		Poco::Stopwatch stopwatch;

		/// Nested(uint8 UInt8, uint64 UInt64, string String)
		Nested nested[n];
		for (size_t i = 0; i < n; ++i)
		{
			for (size_t j = 0; j < sizes[i]; ++j)
			{
				nested[i].uint8.push_back(i * 4 + j);
				nested[i].uint64.push_back(1ULL << (63 - nested[i].uint8.back()));
				nested[i].string.push_back("");
				{
					DB::WriteBufferFromString wb(nested[i].string.back());
					DB::writeIntText(nested[i].uint8.back(), wb);
					DB::writeString("SpAcE", wb);
					DB::writeIntText(nested[i].uint64.back(), wb);
				}
			}
		}
		DB::NamesAndTypesListPtr types = new DB::NamesAndTypesList;
		types->push_back(DB::NameAndTypePair("uint8", new DB::DataTypeUInt8));
		types->push_back(DB::NameAndTypePair("uint64", new DB::DataTypeUInt64));
		types->push_back(DB::NameAndTypePair("string", new DB::DataTypeString));

		DB::DataTypeNested data_type(types);

		{
			DB::ColumnPtr column_p = data_type.createColumn();
			DB::ColumnNested * column = typeid_cast<DB::ColumnNested *>(&*column_p);
			DB::Columns & data = column->getData();
			DB::ColumnNested::Offsets_t & offsets = column->getOffsets();

			data.resize(3);
			data[0] = new DB::ColumnUInt8;
			data[1] = new DB::ColumnUInt64;
			data[2] = new DB::ColumnString;

			for (size_t i = 0; i < n; ++i)
			{
				for (size_t j = 0; j < sizes[i]; ++j)
				{
					data[0]->insert(DB::Field(UInt64(nested[i].uint8[j])));
					data[1]->insert(DB::Field(nested[i].uint64[j]));
					data[2]->insert(DB::Field(nested[i].string[j].data(), nested[i].string[j].size()));
				}
				offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + sizes[i]);
			}

			stopwatch.restart();
			{
				std::ofstream ostr("test.size");
				DB::WriteBufferFromOStream out_buf(ostr);
				data_type.serializeOffsets(*column, out_buf);
			}
			{
				std::ofstream ostr("test");
				DB::WriteBufferFromOStream out_buf(ostr);
				data_type.serializeBinary(*column, out_buf);
			}
			stopwatch.stop();

			std::cout << "Writing, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
		}

		{
			DB::ColumnPtr column_p = data_type.createColumn();
			DB::ColumnNested * column = typeid_cast<DB::ColumnNested *>(&*column_p);

			std::ifstream istr("test");
			DB::ReadBufferFromIStream in_buf(istr);

			stopwatch.restart();
			{
				std::ifstream istr("test.size");
				DB::ReadBufferFromIStream in_buf(istr);
				data_type.deserializeOffsets(*column, in_buf, n);
			}
			{
				std::ifstream istr("test");
				DB::ReadBufferFromIStream in_buf(istr);
				data_type.deserializeBinary(*column, in_buf, n, 0);
			}
			stopwatch.stop();

			std::cout << "Reading, elapsed: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;

			std::cout << std::endl;

			DB::Columns & data = column->getData();
			DB::ColumnNested::Offsets_t & offsets = column->getOffsets();

			Nested res;
			res.uint8.assign(typeid_cast<DB::ColumnUInt8 &>(*data[0]).getData());
			res.uint64.assign(typeid_cast<DB::ColumnUInt64 &>(*data[1]).getData());
			DB::ColumnString & res_string = typeid_cast<DB::ColumnString &>(*data[2]);

			std::cout << "offsets: [";
			for (size_t i = 0; i < offsets.size(); ++i)
			{
				if (i) std::cout << ", ";
				std::cout << offsets[i];
			}
			std::cout << "]\n" << std::endl;

			for (size_t i = 0; i < n; ++i)
			{
				size_t sh = i ? offsets[i - 1] : 0;

				std::cout << "[";
				for (size_t j = 0; j < sizes[i]; ++j)
				{
					if (j) std::cout << ", ";
					std::cout << int(res.uint8[sh + j]);
				}
				std::cout << "]\n";

				std::cout << "[";
				for (size_t j = 0; j < sizes[i]; ++j)
				{
					if (j) std::cout << ", ";
					std::cout << res.uint64[sh + j];
				}
				std::cout << "]\n";

				std::cout << "[";
				for (size_t j = 0; j < sizes[i]; ++j)
				{
					if (j) std::cout << ", ";
					std::cout << '"' << res_string.getDataAt(sh + j).toString() << '"';
				}
				std::cout << "]\n";

				std::cout << std::endl;
			}
		}
	}
	catch (const DB::Exception & e)
	{
		std::cerr << e.what() << ", " << e.displayText() << std::endl;
		return 1;
	}

	return 0;
}
