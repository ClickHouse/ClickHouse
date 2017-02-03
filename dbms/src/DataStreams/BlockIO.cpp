#include <DB/Interpreters/ProcessList.h>
#include <DB/DataStreams/BlockIO.h>

namespace DB
{

BlockIO::~BlockIO()
{
	/// Avoid stream destruction inside ProcessListElement destructor to avoid long locks
	if (process_list_entry)
		(*process_list_entry)->releaseQueryStreams();
}

}
