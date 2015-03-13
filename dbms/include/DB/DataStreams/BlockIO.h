#pragma once

#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Interpreters/ProcessList.h>


namespace DB
{

struct BlockIO
{
	/** process_list_entry должен уничтожаться позже, чем in и out,
	  *  так как внутри in и out есть ссылка на объект внутри process_list_entry
	  *  (MemoryTracker * current_memory_tracker),
	  *  которая может использоваться до уничтожения in и out.
	  */
	ProcessList::EntryPtr process_list_entry;

	BlockInputStreamPtr in;
	BlockOutputStreamPtr out;

	Block in_sample;	/// Пример блока, который будет прочитан из in.
	Block out_sample;	/// Пример блока, которого нужно писать в out.

	BlockIO & operator= (const BlockIO & rhs)
	{
		/// Обеспечиваем правильный порядок уничтожения.
		out 				= nullptr;
		in 					= nullptr;
		process_list_entry 	= nullptr;

		process_list_entry 	= rhs.process_list_entry;
		in 					= rhs.in;
		out 				= rhs.out;
		in_sample 			= rhs.in_sample;
		out_sample 			= rhs.out_sample;

		return *this;
	}
};

}
