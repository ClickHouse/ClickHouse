#include <DB/Common/CurrentMetrics.h>


namespace CurrentMetrics
{
	std::atomic<Value> values[END] {};	/// Глобальная переменная - инициализируется нулями.
}
