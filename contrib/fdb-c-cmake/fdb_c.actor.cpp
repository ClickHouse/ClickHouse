#include <cstdint>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <foundationdb/fdb_c_types.h>
#include <flow/ThreadHelper.actor.h>
#include <flow/network.h>
#include <flow/Arena.h>
#include <flow/flow.h>
#include <flow/actorcompiler.h>

namespace foundationdb::extension {
class ThreadUnsafeRWLock : NonCopyable {
public:
	ThreadUnsafeRWLock() : read(true), count(0) {}

	Future<Void> lock_shared();
	Future<Void> lock();
	void unlock();
	bool isFree() { return read && count == 0; };

	struct Acquire {
		bool read;
		Promise<Void> promise;
	};

private:
	bool read : 1;
	uint32_t count : 31;
	std::queue<Acquire> acquire_queue;
};

Future<Void> ThreadUnsafeRWLock::lock_shared() {
	Promise<Void> promise;
	auto future = promise.getFuture();

	if (acquire_queue.empty() && read) {
		++count; // FIXME: count may overflow
		promise.send(Void());
	} else {
		acquire_queue.emplace(Acquire{ true, std::move(promise) });
	}

	return future;
}

Future<Void> ThreadUnsafeRWLock::lock() {
	Promise<Void> promise;
	auto future = promise.getFuture();

	if (acquire_queue.empty() && read && count == 0) {
		read = false;
		promise.send(Void());
	} else {
		acquire_queue.emplace(Acquire{ false, std::move(promise) });
	}

	return future;
}

void ThreadUnsafeRWLock::unlock() {
	if (read) {
		ASSERT(count != 0);

		--count;
		if (count == 0 && !acquire_queue.empty()) {
			auto& r = acquire_queue.front();
			ASSERT(!r.read);

			read = false;
			r.promise.send(Void());
			acquire_queue.pop();
		}
	} else {
		read = true;

		// No pending acquire
		if (acquire_queue.empty())
			return;

		// Pop one write lock
		{
			auto& r = acquire_queue.front();
			if (!r.read) {
				read = false;
				r.promise.send(Void());
				acquire_queue.pop();
				return;
			}
		}

		// OR pop all read lock
		while (!acquire_queue.empty()) {
			auto& r = acquire_queue.front();
			if (!r.read)
				break;

			++count;
			r.promise.send(Void());
			acquire_queue.pop();
		}
	}
}

struct KeysHolder {
	bool locked = false;
	std::unordered_set<std::string> r_keys;
	std::unordered_set<std::string> w_keys;
};

static std::unordered_map<std::string, ThreadUnsafeRWLock> locks;
ACTOR Future<Void> acquire_locks(KeysHolder* holder) {
	if (holder->locked)
		return Void();

	state std::vector<Future<Void>> futures;
	for (auto& key : holder->r_keys) {
		futures.emplace_back(locks[key].lock_shared());
	}
	for (auto& key : holder->w_keys) {
		futures.emplace_back(locks[key].lock());
	}

	state int i = 0;
	for (; i < futures.size(); ++i) {
		wait(futures[i]);
	}

	holder->locked = true;

	// for (auto& key : holder->r_keys)
	// 	std::cerr << "Shared lock " << key << std::endl;
	// for (auto& key : holder->w_keys)
	// 	std::cerr << "Exclusive lock " << key << std::endl;

	return Void();
}

void release_locks(KeysHolder* holder) {
	if (!holder->locked)
		return;

	for (auto& key : holder->r_keys) {
		// std::cerr << "Unlock shared lock " << key << std::endl;

		auto it = locks.find(key);
		ASSERT(it != locks.end());

		it->second.unlock();
		if (it->second.isFree())
			locks.erase(it);
	}

	for (auto& key : holder->w_keys) {
		// std::cerr << "Unlock exclusive lock " << key << std::endl;

		auto it = locks.find(key);
		ASSERT(it != locks.end());

		it->second.unlock();
		if (it->second.isFree())
			locks.erase(it);
	}
}

extern "C" {
DLLEXPORT void* fdb_rwlocks_create() {
	return new KeysHolder();
}

DLLEXPORT void fdb_rwlocks_shared(void* holder, const char* key) {
	ASSERT(holder);
	auto h = (KeysHolder*)holder;

	auto w_key = h->w_keys.find(key);
	if (w_key != h->w_keys.end())
		return;

	h->r_keys.emplace(key);
}

DLLEXPORT void fdb_rwlocks_exclusive(void* holder, const char* key) {
	ASSERT(holder);
	auto h = (KeysHolder*)holder;

	auto r_key = h->r_keys.find(key);
	if (r_key != h->r_keys.end())
		h->r_keys.erase(key);

	h->w_keys.emplace(key);
}

DLLEXPORT FDBFuture* fdb_rwlocks_lock(void* holder) {
	ASSERT(holder);
	auto h = (KeysHolder*)holder;
	return (FDBFuture*)onMainThread([=]() { return acquire_locks(h); }).extractPtr();
}

DLLEXPORT void fdb_rwlocks_free(void* holder) {
	ASSERT(holder);
	auto h = (KeysHolder*)holder;
	onMainThreadVoid([=]() {
		release_locks(h);
		delete h;
	});
}

DLLEXPORT FDBFuture* fdb_delay(double seconds) {
	return (FDBFuture*)onMainThread([=]() { return g_network->delay(seconds, TaskPriority::DefaultDelay); })
	    .extractPtr();
}
}
} // namespace foundationdb::extension