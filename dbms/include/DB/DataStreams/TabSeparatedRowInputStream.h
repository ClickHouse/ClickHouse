#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/IO/ReadBuffer.h>
#include <DB/DataStreams/IRowInputStream.h>


namespace DB
{

/** Поток для ввода данных в формате tsv.
  */
class TabSeparatedRowInputStream : public IRowInputStream
{
public:
	/** with_names - в первой строке заголовок с именами столбцов
	  * with_types - на следующей строке заголовок с именами типов
	  */
	TabSeparatedRowInputStream(ReadBuffer & istr_, const Block & sample_, bool with_names_ = false, bool with_types_ = false);

	bool read(Row & row) override;
	void readPrefix() override;

	/** В случае исключения при парсинге, вы можете вызвать эту функцию.
	  * Она выполняет заново парсинг последних двух строк и выводит подробную информацию о том, что происходит.
	  */
	void printDiagnosticInfo(WriteBuffer & out);

private:
	ReadBuffer & istr;
	const Block sample;
	bool with_names;
	bool with_types;
	DataTypes data_types;

	/// Для удобной диагностики в случае ошибки.

	size_t row_num = 0;

	/// Сколько байт было считано, не считая тех, что ещё в буфере.
	size_t bytes_read_at_start_of_buffer_on_current_row = 0;
	size_t bytes_read_at_start_of_buffer_on_prev_row = 0;

	BufferBase::Position pos_of_current_row = nullptr;
	BufferBase::Position pos_of_prev_row = nullptr;

	void updateDiagnosticInfo()
	{
		++row_num;

		bytes_read_at_start_of_buffer_on_prev_row = bytes_read_at_start_of_buffer_on_current_row;
		bytes_read_at_start_of_buffer_on_current_row = istr.count() - istr.offset();

		pos_of_prev_row = pos_of_current_row;
		pos_of_current_row = istr.position();
	}

	bool parseRowAndPrintDiagnosticInfo(WriteBuffer & out);
};

}
