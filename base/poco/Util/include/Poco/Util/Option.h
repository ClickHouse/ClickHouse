//
// Option.h
//
// Library: Util
// Package: Options
// Module:  Option
//
// Definition of the Option class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_Option_INCLUDED
#define Util_Option_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/OptionCallback.h"


namespace Poco {
namespace Util {


class Application;
class Validator;
class AbstractConfiguration;


class Util_API Option
	/// This class represents and stores the properties
	/// of a command line option.
	///
	/// An option has a full name, an optional short name,
	/// a description (used for printing a usage statement),
	/// and an optional argument name.
	/// An option can be optional or required.
	/// An option can be repeatable, which means that it can
	/// be given more than once on the command line.
	///
	/// An option can be part of an option group. At most one
	/// option of each group may be specified on the command
	/// line.
	///
	/// An option can be bound to a configuration property.
	/// In this case, a configuration property will automatically
	/// receive the option's argument value.
	///
	/// A callback method can be specified for options. This method
	/// is called whenever an option is specified on the command line.
	///
	/// Option argument values can be automatically validated using a
	/// Validator.
	///
	/// Option instances are value objects.
	///
	/// Typically, after construction, an Option object is immediately
	/// passed to an Options object.
	///
	/// An Option object can be created by chaining the constructor
	/// with any of the setter methods, as in the following example:
	///
	///     Option versionOpt("include", "I", "specify an include directory")
	///        .required(false)
	///        .repeatable(true)
	///        .argument("directory");
{
public:
	Option();
		/// Creates an empty Option.

	Option(const Option& option);
		/// Creates an option from another one.

	Option(const std::string& fullName, const std::string& shortName);
		/// Creates an option with the given properties.

	Option(const std::string& fullName, const std::string& shortName, const std::string& description, bool required = false);
		/// Creates an option with the given properties.

	Option(const std::string& fullName, const std::string& shortName, const std::string& description, bool required, const std::string& argName, bool argRequired = false);
		/// Creates an option with the given properties.

	~Option();
		/// Destroys the Option.

	Option& operator = (const Option& option);
		/// Assignment operator.
		
	void swap(Option& option);
		/// Swaps the option with another one.

	Option& shortName(const std::string& name);
		/// Sets the short name of the option.
		
	Option& fullName(const std::string& name);
		/// Sets the full name of the option.
		
	Option& description(const std::string& text);
		/// Sets the description of the option.
		
	Option& required(bool flag);
		/// Sets whether the option is required (flag == true)
		/// or optional (flag == false).

	Option& repeatable(bool flag);
		/// Sets whether the option can be specified more than once
		/// (flag == true) or at most once (flag == false).
		
	Option& argument(const std::string& name, bool required = true);
		/// Specifies that the option takes an (optional or required)
		/// argument.
		
	Option& noArgument();
		/// Specifies that the option does not take an argument (default).

	Option& group(const std::string& group);
		/// Specifies the option group the option is part of.
		
	Option& binding(const std::string& propertyName);
		/// Binds the option to the configuration property with the given name.
		///
		/// The configuration will automatically receive the option's argument.

	Option& binding(const std::string& propertyName, AbstractConfiguration* pConfig);
		/// Binds the option to the configuration property with the given name, 
		/// using the given AbstractConfiguration.
		///
		/// The configuration will automatically receive the option's argument.
		
	Option& callback(const AbstractOptionCallback& cb);
		/// Binds the option to the given method.
		///
		/// The callback method will be called when the option
		/// has been specified on the command line.
		///
		/// Usage:
		///     callback(OptionCallback<MyApplication>(this, &MyApplication::myCallback));

	Option& validator(Validator* pValidator);
		/// Sets the validator for the given option.
		///
		/// The Option takes ownership of the Validator and
		/// deletes it when it's no longer needed.

	const std::string& shortName() const;
		/// Returns the short name of the option.
		
	const std::string& fullName() const;
		/// Returns the full name of the option.
		
	const std::string& description() const;
		/// Returns the description of the option.
		
	bool required() const;
		/// Returns true if the option is required, false if not.
	
	bool repeatable() const;
		/// Returns true if the option can be specified more than
		/// once, or false if at most once.
	
	bool takesArgument() const;
		/// Returns true if the options takes an (optional) argument.
		
	bool argumentRequired() const;
		/// Returns true if the argument is required.

	const std::string& argumentName() const;
		/// Returns the argument name, if specified.
		
	const std::string& group() const;
		/// Returns the option group the option is part of,
		/// or an empty string, if the option is not part of
		/// a group.
		
	const std::string& binding() const;
		/// Returns the property name the option is bound to,
		/// or an empty string in case it is not bound.
		
	AbstractOptionCallback* callback() const;
		/// Returns a pointer to the callback method for the option,
		/// or NULL if no callback has been specified.
		
	Validator* validator() const;
		/// Returns the option's Validator, if one has been specified,
		/// or NULL otherwise.	
		
	AbstractConfiguration* config() const;
		/// Returns the configuration, if specified, or NULL otherwise.
		
	bool matchesShort(const std::string& option) const;
		/// Returns true if the given option string matches the
		/// short name.
		///
		/// The first characters of the option string must match
		/// the short name of the option (case sensitive),
		/// or the option string must partially match the full
		/// name (case insensitive).

	bool matchesFull(const std::string& option) const;
		/// Returns true if the given option string matches the
		/// full name.
		///
		/// The option string must match the full
		/// name (case insensitive).

	bool matchesPartial(const std::string& option) const;
		/// Returns true if the given option string partially matches the
		/// full name.
		///
		/// The option string must partially match the full
		/// name (case insensitive).
		
	void process(const std::string& option, std::string& arg) const;
		/// Verifies that the given option string matches the
		/// requirements of the option, and extracts the option argument,
		/// if present.
		///
		/// If the option string is okay and carries an argument,
		/// the argument is returned in arg.
		///
		/// Throws a MissingArgumentException if a required argument
		/// is missing. Throws an UnexpectedArgumentException if an
		/// argument has been found, but none is expected. 

private:
	std::string _shortName;
	std::string _fullName;
	std::string _description;
	bool        _required;
	bool        _repeatable;
	std::string _argName;
	bool        _argRequired;
	std::string _group;
	std::string _binding;
	Validator*  _pValidator;
	AbstractOptionCallback* _pCallback;
	AbstractConfiguration*  _pConfig;
};


//
// inlines
//


inline const std::string& Option::shortName() const
{
	return _shortName;
}

	
inline const std::string& Option::fullName() const
{
	return _fullName;
}

	
inline const std::string& Option::description() const
{
	return _description;
}

	
inline bool Option::required() const
{
	return _required;
}


inline bool Option::repeatable() const
{
	return _repeatable;
}

	
inline bool Option::takesArgument() const
{
	return !_argName.empty();
}

	
inline bool Option::argumentRequired() const
{
	return _argRequired;
}


inline const std::string& Option::argumentName() const
{
	return _argName;
}


inline const std::string& Option::group() const
{
	return _group;
}


inline const std::string& Option::binding() const
{
	return _binding;
}


inline AbstractOptionCallback* Option::callback() const
{
	return _pCallback;
}


inline Validator* Option::validator() const
{
	return _pValidator;
}


inline AbstractConfiguration* Option::config() const
{
	return _pConfig;
}


} } // namespace Poco::Util


#endif // Util_Option_INCLUDED
