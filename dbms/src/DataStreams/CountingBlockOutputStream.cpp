#include <DB/DataStreams/CountingBlockOutputStream.h>
#include <DB/Common/ProfileEvents.h>


namespace ProfileEvents
{
	extern const Event InsertedRows;
	extern const Event InsertedBytes;
}


namespace DB
{

void CountingBlockOutputStream::write(const Block & block)
{
	stream->write(block);

	Progress local_progress(block.rowsInFirstColumn(), block.bytes(), 0);
	progress.incrementPiecewiseAtomically(local_progress);

	ProfileEvents::increment(ProfileEvents::InsertedRows, local_progress.rows);
	ProfileEvents::increment(ProfileEvents::InsertedBytes, local_progress.bytes);

	if (process_elem)
		process_elem->updateProgressOut(local_progress);

	if (progress_callback)
		progress_callback(local_progress);
}

}
