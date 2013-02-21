#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>


namespace DB
{
	
	
	/** Проверить, существует ли таблица. Вернуть одну строку с одним столбцом result типа UInt8 со значением 0 или 1.
	 */
	class InterpreterDescribeQuery
	{
	public:
		InterpreterDescribeQuery(ASTPtr query_ptr_, Context & context_)
			: query_ptr(query_ptr_), context(context_) {}
		
		BlockIO execute()
		{
			BlockIO res;
			res.in = executeImpl();
			res.in_sample = getSampleBlock();
			
			return res;
		}
		
		BlockInputStreamPtr executeAndFormat(WriteBuffer & buf)
		{
			Block sample = getSampleBlock();
			ASTPtr format_ast = dynamic_cast<ASTDescribeQuery &>(*query_ptr).format;
			String format_name = format_ast ? dynamic_cast<ASTIdentifier &>(*format_ast).name : "TabSeparated";
			
			BlockInputStreamPtr in = executeImpl();
			BlockOutputStreamPtr out = context.getFormatFactory().getOutput(format_name, buf, sample);
			
			copyData(*in, *out);
			
			return in;
		}
		
	private:
		ASTPtr query_ptr;
		Context context;
		
		Block getSampleBlock()
		{
			Block block;
			
			ColumnWithNameAndType col;
			col.name = "name";
			col.type = new DataTypeString;
			
			block.insert(col);
			
			col.name = "type";
			
			block.insert(col);
			
			return block;
		}
		
		BlockInputStreamPtr executeImpl()
		{
			const ASTDescribeQuery & ast = dynamic_cast<const ASTDescribeQuery &>(*query_ptr);
			
			NamesAndTypesList columns;
			
			{
				Poco::ScopedLock<Poco::Mutex> lock(context.getMutex());
				
				if (!context.isTableExist(ast.database, ast.table))
					throw Exception("Table " + (ast.database.empty() ? "" : ast.database + ".") + ast.table + " doesn't exist", ErrorCodes::UNKNOWN_TABLE);
				
				columns = context.getTable(ast.database, ast.table)->getColumnsList();
			}
			
			ColumnString * name_column = new ColumnString;
			ColumnString * type_column = new ColumnString;
			
			Block block;
			block.insert(ColumnWithNameAndType(name_column, new DataTypeString, "name"));
			block.insert(ColumnWithNameAndType(type_column, new DataTypeString, "type"));
			
			for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
			{
				name_column->insert(it->first);
				type_column->insert(it->second->getName());
			}
			
			return new OneBlockInputStream(block);
		}
	};
	
	
}
