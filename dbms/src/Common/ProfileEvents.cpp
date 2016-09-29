#include <DB/Common/ProfileEvents.h>


namespace ProfileEvents
{
	std::atomic<size_t> counters[END] {};	/// Глобальная переменная - инициализируется нулями.
}
