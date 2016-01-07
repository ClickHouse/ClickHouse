//
// AbstractPreparation.h
//
// $Id: //poco/Main/Data/include/Poco/Data/AbstractPreparation.h#4 $
//
// Library: Data
// Package: DataCore
// Module:  AbstractPreparation
//
// Definition of the AbstractPreparation class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_AbstractPreparation_INCLUDED
#define Data_AbstractPreparation_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractPreparator.h"
#include "Poco/SharedPtr.h"
#include <cstddef>


namespace Poco {
namespace Data {


class Data_API AbstractPreparation
	/// Interface for calling the appropriate AbstractPreparator method
{
public:
	typedef SharedPtr<AbstractPreparation> Ptr;
	typedef AbstractPreparator::Ptr PreparatorPtr;

	AbstractPreparation(PreparatorPtr pPreparator);
		/// Creates the AbstractPreparation.

	virtual ~AbstractPreparation();
		/// Destroys the AbstractPreparation.

	virtual void prepare() = 0;
		/// Prepares data.

protected:
	AbstractPreparation();
	AbstractPreparation(const AbstractPreparation&);
	AbstractPreparation& operator = (const AbstractPreparation&);

	PreparatorPtr preparation();
		/// Returns the preparation object

	PreparatorPtr _pPreparator;
};


//
// inlines
//
inline AbstractPreparation::PreparatorPtr AbstractPreparation::preparation()
{
	return _pPreparator;
}


} } // namespace Poco::Data


#endif // Data_AbstractPreparation_INCLUDED
