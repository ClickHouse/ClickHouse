//
// Platform_VMS.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Platform_VMS.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  Platform
//
// Platform and architecture identification macros
// and platform-specific definitions for OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Platform_VMS_INCLUDED
#define Foundation_Platform_VMS_INCLUDED


// Define the POCO_DESCRIPTOR_STRING and POCO_DESCRIPTOR_LITERAL
// macros which we use instead of $DESCRIPTOR and $DESCRIPTOR64. 
// Our macros work with both 32bit and 64bit pointer sizes.
#if __INITIAL_POINTER_SIZE != 64
	#define POCO_DESCRIPTOR_STRING(name, string) \
		struct dsc$descriptor_s name =	\
		{								\
			string.size(),				\
			DSC$K_DTYPE_T,				\
			DSC$K_CLASS_S,				\
			(char*) string.data()		\
		}
	#define POCO_DESCRIPTOR_LITERAL(name, string) \
		struct dsc$descriptor_s name =	\
		{								\
			sizeof(string) - 1,			\
			DSC$K_DTYPE_T,				\
			DSC$K_CLASS_S,				\
			(char*) string				\
		}
#else
	#define POCO_DESCRIPTOR_STRING(name, string) \
		struct dsc64$descriptor_s name =\
		{								\
			1,							\
			DSC64$K_DTYPE_T,			\
			DSC64$K_CLASS_S,			\
			-1,							\
			string.size(),				\
			(char*) string.data()		\
		}
	#define POCO_DESCRIPTOR_LITERAL(name, string) \
		struct dsc64$descriptor_s name =\
		{								\
			1,							\
			DSC64$K_DTYPE_T,			\
			DSC64$K_CLASS_S,			\
			-1,							\
			sizeof(string) - 1,			\
			(char*) string				\
		}
#endif


// No <sys/select.h> header file
#define POCO_NO_SYS_SELECT_H


#endif // Foundation_Platform_VMS_INCLUDED
