#include <iostream>
#include <Poco/Stopwatch.h>

#include <DB/Table.h>
#include <DB/Column.h>
#include <DB/ColumnType.h>
#include <DB/PrimaryKeyNone.h>
#include <DB/RowSet.h>


int main(int argc, char ** argv)
{
	Poco::Stopwatch stopwatch;
	
	/// создаём таблицу
	DB::Column column0;
	column0.name = "ID";
	column0.type = new DB::ColumnTypeVarUInt;

	DB::Column column1;
	column1.name = "PageViews";
	column1.type = new DB::ColumnTypeVarUInt;

	DB::Column column2;
	column2.name = "URL";
	column2.type = new DB::ColumnTypeText;

	Poco::SharedPtr<DB::Table::Columns> columns = new DB::Table::Columns;
	columns->push_back(column0);
	columns->push_back(column1);
	columns->push_back(column2);

	Poco::SharedPtr<DB::Table::ColumnNumbers> primary_key_column_numbers = new DB::Table::ColumnNumbers;
	primary_key_column_numbers->push_back(0);

	DB::ColumnGroup column_group0;
	column_group0.column_numbers.push_back(0);
	column_group0.column_numbers.push_back(1);
	column_group0.column_numbers.push_back(2);
	column_group0.primary_key = new DB::PrimaryKeyNone("./", "TestPrimaryKeyNone");

	Poco::SharedPtr<DB::Table::ColumnGroups> column_groups = new DB::Table::ColumnGroups;
	column_groups->push_back(column_group0);

	DB::Table table("TestTable", columns, primary_key_column_numbers, column_groups);

	/// создаём набор данных
	DB::AggregatedRowSet data;
	std::string text("http://www.google.com/custom?cof=LW%3A277%3BL%3Ahttp%3A%2F%2Fwww.boost.org%2Fboost.png%3BLH%3A86%3BAH%3Acenter%3BGL%3A0%3BS%3Ahttp%3A%2F%2Fwww.boost.org%3BAWFID%3A9b83d16ce652ed5a%3B&sa=Google+Search&domains=www.boost.org%3Blists.boost.org&hq=site%3Awww.boost.org+OR+site%3Alists.boost.org&q=boost%3A%3Ablank");
	{
		DB::Row key;
		key.push_back(DB::Field(DB::UInt(0)));
		key.push_back(DB::Field(DB::UInt(0)));
		key.push_back(DB::Field(DB::String("")));

		DB::Row value;

		stopwatch.restart();

		for (DB::UInt i = 0; i < 1000000; ++i)
		{
			key[0] = i;
			key[1] = i * 123456789 % 1000000;
			key[2] = text;

			data[key] = value;
		}

		stopwatch.stop();
		std::cout << "Filling data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// заполняем таблицу
	{
		DB::ColumnMask mask(3, true);
	
		stopwatch.restart();

		column_group0.primary_key->merge(data, mask);

		stopwatch.stop();
		std::cout << "Saving data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// читаем таблицу
	DB::AggregatedRowSet data_read;
	{
		DB::Row key;
		Poco::SharedPtr<DB::ITablePartReader> reader(column_group0.primary_key->read(key));
		
		stopwatch.restart();

		DB::Row row;
		DB::UInt i = 0;
		while (reader->fetch(row))
		{
			if (boost::get<DB::UInt>(row[0]) != i
				|| boost::get<DB::UInt>(row[1]) != i * 123456789 % 1000000
				|| boost::get<DB::String>(row[2]) != text)
				throw Poco::Exception("Incorrect data");
			++i;
		}
		if (i != 1000000)
			throw Poco::Exception("Number of rows doesn't match");

		stopwatch.stop();
		std::cout << "Reading data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
