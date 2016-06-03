//
// OpcomChannel.h
//
// $Id: //poco/1.4/Foundation/include/Poco/OpcomChannel.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  OpcomChannel
//
// Definition of the OpcomChannel class specific to OpenVMS.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_OpcomChannel_INCLUDED
#define Foundation_OpcomChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"


namespace Poco {


class Foundation_API OpcomChannel: public Channel
	/// A OpenVMS-only channel that uses the OpenVMS OPCOM service.
{
public:
	OpcomChannel();
		/// Creates an OpcomChannel that uses the OPC$M_NM_CENTRL
		/// target.
		
	OpcomChannel(int target);
		/// Creates an OpcomChannel that uses the given target.
		/// Specify one of the OPC$M_NM_* values.
		/// See also setProperty().
		
	void log(const Message& msg);
		/// Logs the given message using the OpenVMS OPCOM service.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given name. 
		/// 
		/// The only supported property is "target", which must
		/// be set to one of the following values:
		///
		///   * CARDS:    Card device operator
		///   * CENTRL:   Central operator
		///   * SECURITY: Security operator
		///   * CLUSTER:  OpenVMS Cluster operator
		///   * DEVICE:   Device status information
		///   * DISKS:    Disks operator
		///   * NTWORK:   Network operator
		///   * TAPES:    Tapes operator
		///   * PRINT:    Printer operator
		///   * OPER1 .. 
		///   * OPER12:   System-manager-defined operator functions

	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.
		/// See setProperty() for a description of the supported
		/// properties.

	static const std::string PROP_TARGET;
		
protected:
	~OpcomChannel();
	
private:
	int _target;
};


} // namespace Poco


#endif // Foundation_OpcomChannel_INCLUDED
