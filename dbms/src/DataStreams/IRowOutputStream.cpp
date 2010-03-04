#include <DB/DataStreams/IRowOutputStream.h>


namespace DB
{

void IRowOutputStream::write(const Row & row)
{
	writeRowStartDelimiter();

	if (!row.empty())
	{
		writeField(row[0]);
		for (size_t i = 1; i < row.size(); ++i)
		{
			writeFieldDelimiter();
			writeField(row[i]);
		}
	}
		
	writeRowEndDelimiter();
}

}
