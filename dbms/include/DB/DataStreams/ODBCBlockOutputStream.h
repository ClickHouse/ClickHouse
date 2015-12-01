#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Формат данных, предназначенный для упрощения реализации ODBC драйвера.
  * ODBC драйвер предназначен для сборки под разные платформы без зависимостей от основного кода,
  *  поэтому формат сделан так, чтобы в нём можно было как можно проще его распарсить.
  * Выводится заголовок с нужной информацией.
  * Затем данные выводятся в порядке строк. Каждое значение выводится так: длина в формате VarUInt, затем данные в текстовом виде.
  */
class ODBCBlockOutputStream : public IBlockOutputStream
{
public:
	ODBCBlockOutputStream(WriteBuffer & out_);

	void write(const Block & block) override;

	void flush() override { out.next(); }
	String getContentType() const override { return "application/octet-stream"; }

private:
	bool is_first = true;
	WriteBuffer & out;
};

}
