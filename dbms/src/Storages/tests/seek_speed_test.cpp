#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/Common/Stopwatch.h>
#include <Poco/File.h>
#include <iomanip>
#include <vector>
#include <algorithm>

/** Проверяем гипотезу, что пропуск ненужных кусков seek-ом вперед никогда не ухудшает общую скорость чтения.
  * Перед измерениями желательно выбросить дисковый кэш: `echo 3 > /proc/sys/vm/drop_caches`.
  *
  * Результат: да, даже частые относительно короткие seek вперед ничего не ухудшают на всех опробованных параметрах:
  * - 1MiB данных, 16 0 0 16 vs 16 16 32 16
  * - 1GiB данных, 1048576 0 0 vs 1048576 512 1024 vs 1048576 1048576 1048576
  * - 1GiB данных, 1024 0 0 vs 1024 512 1024
  */

int main(int argc, const char ** argv)
{
	if (argc < 5 || argc > 6)
	{
		std::cerr << "Usage:\n"
			<< argv[0] << " file bytes_in_block min_skip_bytes max_skip_bytes [buffer_size]" << std::endl;
		return 0;
	}

	int block = atoi(argv[2]);
	int min_skip = atoi(argv[3]);
	int max_skip = atoi(argv[4]);
	size_t buf_size = argc <= 5 ? DBMS_DEFAULT_BUFFER_SIZE : (size_t)atoi(argv[5]);

	UInt64 size = Poco::File(argv[1]).getSize();
	UInt64 pos = 0;
	DB::ReadBufferFromFile in(argv[1], buf_size);
	char * buf = new char[block];
	int checksum = 0;
	UInt64 bytes_read = 0;

	Stopwatch watch;

	while (!in.eof())
	{
		UInt64 len = (UInt64)(rand() % (max_skip - min_skip + 1) + min_skip);
		len = std::min(len, size - pos);
		off_t seek_res = in.seek(len, SEEK_CUR);
		pos += len;
		if (seek_res != static_cast<off_t>(pos))
		{
			std::cerr << "Unexpected seek return value: " << seek_res << "; expeted " << pos << ", seeking by " << len << std::endl;
			return 1;
		}
		len = std::min((UInt64)block, size - pos);
		in.read(&buf[0], len);
		checksum += buf[0] + buf[block - 1];
		pos += len;
		bytes_read += len;
	}
	watch.stop();

	std::cout << checksum << std::endl;	/// don't optimize

	std::cout << "Read " << bytes_read << " out of " << size << " bytes in "
		<< std::setprecision(4) << watch.elapsedSeconds() << " seconds ("
		<< bytes_read / watch.elapsedSeconds() / 1000000 << " MB/sec.)" << std::endl;

	return 0;
}
