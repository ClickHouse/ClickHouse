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

	Poco::SharedPtr<DB::Table::ColumnGroups> column_groups = new DB::Table::ColumnGroups;
	for (size_t i = 0; i < columns->size(); ++i)
	{
		DB::ColumnGroup column_group;
	 	column_group.column_numbers.push_back(i);
		column_group.storage = new DB::StorageNoKey("./", "TestStorageNoKeyColumn" + Poco::NumberFormatter::format(i));
		column_groups->push_back(column_group);
	}

	DB::Table table("TestTable", columns, primary_key_column_numbers, column_groups);

	/// создаём набор данных
	std::vector<DB::AggregatedRowSet> data(columns->size());
	std::vector<DB::Row> value(columns->size());

	value[0].push_back(DB::Field(DB::UInt(65765691660ULL)));
	value[1].push_back(DB::Field(DB::UInt(20090724165002400ULL)));
	value[2].push_back(DB::Field(DB::UInt(9154640)));
	value[3].push_back(DB::Field(DB::UInt(1)));
	value[4].push_back(DB::Field(DB::UInt(0)));
	value[5].push_back(DB::Field(DB::String("Китайские ученые перепрограммировали клетки и создали из них мышей. Иностранная пресса о событиях в ")));
	value[6].push_back(DB::Field(DB::Int(1)));
	value[7].push_back(DB::Field(DB::UInt(1248456711)));
	value[8].push_back(DB::Field(DB::UInt(71551)));
	value[9].push_back(DB::Field(DB::UInt(1220865079)));
	value[10].push_back(DB::Field(DB::UInt(84)));
	value[11].push_back(DB::Field(DB::UInt(5243575589842965681ULL)));
	value[12].push_back(DB::Field(DB::UInt(0)));
	value[13].push_back(DB::Field(DB::UInt(1)));
	value[14].push_back(DB::Field(DB::UInt(3)));
	value[15].push_back(DB::Field(DB::UInt(5)));
	value[16].push_back(DB::Field(DB::String("http://www.example.ru/wsj/2009/07/24/15:10:00/mouse")));
	value[17].push_back(DB::Field(DB::String("http://www.example.com/")));
	value[18].push_back(DB::Field(DB::UInt(0)));
	value[19].push_back(DB::Field(DB::UInt(1024)));
	value[20].push_back(DB::Field(DB::UInt(768)));
	value[21].push_back(DB::Field(DB::UInt(16)));
	value[22].push_back(DB::Field(DB::UInt(10)));
	value[23].push_back(DB::Field(DB::UInt(0)));
	value[24].push_back(DB::Field(DB::String("")));
	value[25].push_back(DB::Field(DB::UInt(3)));
	value[26].push_back(DB::Field(DB::UInt(5)));
	value[27].push_back(DB::Field(DB::UInt(8)));
	value[28].push_back(DB::Field(DB::String("0 ")));
	value[29].push_back(DB::Field(DB::UInt(1)));
	value[30].push_back(DB::Field(DB::UInt(1)));
	value[31].push_back(DB::Field(DB::UInt(0)));
	value[32].push_back(DB::Field(DB::UInt(0)));
	value[33].push_back(DB::Field(DB::String("")));
	value[34].push_back(DB::Field(DB::String("")));
	value[35].push_back(DB::Field(DB::UInt(0)));
	value[36].push_back(DB::Field(DB::Int(1)));
	value[37].push_back(DB::Field(DB::UInt(0)));
	value[38].push_back(DB::Field(DB::String("")));
	value[39].push_back(DB::Field(DB::UInt(0)));
	
	{
		stopwatch.restart();

		for (DB::UInt i = 0; i < 1000000; ++i)
		{
			for (size_t i = 0; i < columns->size(); ++i)
				data[i][value[0]] = value[i];
			++boost::get<DB::UInt>(value[0][0]);
		}

		stopwatch.stop();
		std::cout << "Filling data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// заполняем таблицу
	{
		DB::ColumnMask mask(2);
		mask[0] = false;
		mask[1] = true;
	
		stopwatch.restart();

		Poco::Stopwatch column_stopwatch;
		for (size_t i = 0; i < column_groups->size(); ++i)
		{
			column_stopwatch.restart();
			(*column_groups)[i].storage->merge(data[i], mask);
			column_stopwatch.stop();
			std::cout << "Saving column" << i << ": " << static_cast<double>(column_stopwatch.elapsed()) / 1000000 << std::endl;
		}

		stopwatch.stop();
		std::cout << "Saving data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	/// читаем таблицу
	{
		DB::Row key;
		stopwatch.restart();
		DB::Row row;

		Poco::Stopwatch column_stopwatch;
		for (size_t i = 0; i < column_groups->size(); ++i)
		{
			column_stopwatch.restart();

			Poco::SharedPtr<DB::ITablePartReader> reader((*column_groups)[i].storage->read(key));	/// UniqID
			DB::UInt row_num = 0;
			while (reader->fetch(row))
			{
				++row_num;
			}
			if (row_num != 1000000)
				throw Poco::Exception("Number of rows doesn't match");

			column_stopwatch.stop();
			std::cout << "Reading column" << i << ": " << static_cast<double>(column_stopwatch.elapsed()) / 1000000 << std::endl;
		}
		
		stopwatch.stop();
		std::cout << "Reading data: " << static_cast<double>(stopwatch.elapsed()) / 1000000 << std::endl;
	}

	return 0;
}
