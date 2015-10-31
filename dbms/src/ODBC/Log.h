#pragma once


#define LOG(message) \
	do \
	{ \
		std::cerr << message << std::endl; \
	} \
	while (false)
