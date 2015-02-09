#pragma once

#include <memory>

namespace DB
{

template <typename T> struct release
{
	void operator()(const T * const ptr) { ptr->release(); }
};

template <typename T> using config_ptr_t = std::unique_ptr<T, release<T>>;

}
