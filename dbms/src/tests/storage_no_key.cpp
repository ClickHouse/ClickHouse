#include <iostream>
#include <Poco/Stopwatch.h>

#include <DB/Table.h>
#include <DB/Column.h>
#include <DB/ColumnType.h>
#include <DB/StorageNoKey.h>
#include <DB/RowSet.h>


int main(int argc, char ** argv)
{
	Poco::Stopwatch stopwatch;
	
	/// создаём таблицу

	Poco::SharedPtr<DB::Table::Columns> columns = new DB::Table::Columns;
	
	columns->push_back(DB::Column("WatchID", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("ChunkID", 			new DB::ColumnTypeUInt64));
	columns->push_back(DB::Column("Random", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("JavaEnable", 		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("FrameEnable", 		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("Title", 				new DB::ColumnTypeText));
	columns->push_back(DB::Column("GoodEvent", 			new DB::ColumnTypeVarInt));
	columns->push_back(DB::Column("EventTime", 			new DB::ColumnTypeUInt32));
	columns->push_back(DB::Column("CounterID", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("ClientIP", 			new DB::ColumnTypeUInt32));
	columns->push_back(DB::Column("RegionID", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("UniqID", 			new DB::ColumnTypeUInt64));
	columns->push_back(DB::Column("SessID", 			new DB::ColumnTypeUInt32));
	columns->push_back(DB::Column("CounterClass",	 	new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("OS", 				new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("UserAgent", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("URL", 				new DB::ColumnTypeText));
	columns->push_back(DB::Column("Referer", 			new DB::ColumnTypeText));
	columns->push_back(DB::Column("Refresh", 			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("ResolutionWidth", 	new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("ResolutionHeight",	new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("ResolutionDepth", 	new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("FlashMajor",			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("FlashMinor",			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("FlashMinor2",		new DB::ColumnTypeText));
	columns->push_back(DB::Column("NetMajor",			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("NetMinor",			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("UserAgentMajor",		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("UserAgentMinor",		new DB::ColumnTypeFixedText(2)));
	columns->push_back(DB::Column("CookieEnable",		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("JavascriptEnable",	new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("IsMobile",			new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("MobilePhone",		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("MobilePhoneModel",	new DB::ColumnTypeText));
	columns->push_back(DB::Column("Params",				new DB::ColumnTypeText));
	columns->push_back(DB::Column("IPNetworkID",		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("TraficSourceID",		new DB::ColumnTypeVarInt));
	columns->push_back(DB::Column("SearchEngineID",		new DB::ColumnTypeVarUInt));
	columns->push_back(DB::Column("SearchPhrase",		new DB::ColumnTypeText));
	columns->push_back(DB::Column("AdvEngineID",		new DB::ColumnTypeVarUInt));

	Poco::SharedPtr<DB::Table::ColumnNumbers> primary_key_column_numbers = new DB::Table::ColumnNumbers;
	primary_key_column_numbers->push_back(0);

	DB::ColumnGroup column_group0;
 	for (size_t i = 0; i < columns->size(); ++i)
		column_group0.column_numbers.push_back(i);

	column_group0.storage = new DB::StorageNoKey("./", "TestStorageNoKey");

	Poco::SharedPtr<DB::Table::ColumnGroups> column_groups = new DB::Table::ColumnGroups;
	column_groups->push_back(column_group0);

	DB::Table table("TestTable", columns, primary_key_column_numbers, column_groups);

	/// создаём набор данных
	DB::AggregatedRowSet data;
	DB::Row key;
	DB::Row value;

	key.push_back(DB::Field(DB::UInt(65765691660ULL)));
	value.push_back(DB::Field(DB::UInt(20090724165002400ULL)));
	value.push_back(DB::Field(DB::UInt(9154640)));
	value.push_back(DB::Field(DB::UInt(1)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::String("Китайские ученые перепрограммировали клетки и создали из них мышей. Иностранная пресса о событиях в ")));
	value.push_back(DB::Field(DB::Int(1)));
	value.push_back(DB::Field(DB::UInt(1248456711)));
	value.push_back(DB::Field(DB::UInt(71551)));
	value.push_back(DB::Field(DB::UInt(1220865079)));
	value.push_back(DB::Field(DB::UInt(84)));
	value.push_back(DB::Field(DB::UInt(5243575589842965681ULL)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::UInt(1)));
	value.push_back(DB::Field(DB::UInt(3)));
	value.push_back(DB::Field(DB::UInt(5)));
	value.push_back(DB::Field(DB::String("http://www.example.ru/wsj/2009/07/24/15:10:00/mouse")));
	value.push_back(DB::Field(DB::String("http://www.example.com/")));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::UInt(1024)));
	value.push_back(DB::Field(DB::UInt(768)));
	value.push_back(DB::Field(DB::UInt(16)));
	value.push_back(DB::Field(DB::UInt(10)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::String("")));
	value.push_back(DB::Field(DB::UInt(3)));
	value.push_back(DB::Field(DB::UInt(5)));
	value.push_back(DB::Field(DB::UInt(8)));
	value.push_back(DB::Field(DB::String("0 ")));
	value.push_back(DB::Field(DB::UInt(1)));
	value.push_back(DB::Field(DB::UInt(1)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::String("")));
	value.push_back(DB::Field(DB::String("")));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::Int(1)));
	value.push_back(DB::Field(DB::UInt(0)));
	value.push_back(DB::Field(DB::String("")));
	value.push_back(DB::Field(DB::UInt(0)));
	
	{
		stopwatch.restart();

		for (DB::UInt i = 0; i < 1000000; ++i)
		{
			data[key] = value;
			++boost::get<DB::UInt>(key[0]);
		}

		stopwatch.stop();
		std::cout << "Filling data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// заполняем таблицу
	{
		DB::ColumnMask mask(columns->size(), true);
	
		stopwatch.restart();

		column_group0.storage->merge(data, mask);

		stopwatch.stop();
		std::cout << "Saving data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// читаем таблицу
	{
		DB::Row key;
		Poco::SharedPtr<DB::ITablePartReader> reader(column_group0.storage->read(key));
		
		stopwatch.restart();

		DB::UInt i = 0;
		DB::Row row;
		while (reader->fetch(row))
		{
			++i;
		}
		if (i != 1000000)
		{
			std::cerr << i << std::endl;
			throw Poco::Exception("Number of rows doesn't match");
		}

		stopwatch.stop();
		std::cout << "Reading data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
