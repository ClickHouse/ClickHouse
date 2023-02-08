//
// Path.h
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Definition of the Path class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Path_INCLUDED
#define Foundation_Path_INCLUDED


#include "Poco/Foundation.h"
#include <vector>


namespace Poco {


class Foundation_API Path
	/// This class represents filesystem paths in a 
	/// platform-independent manner.
	/// Unix, Windows and OpenVMS all use a different
	/// syntax for filesystem paths.
	/// This class can work with all three formats.
	/// A path is made up of an optional node name
	/// (only Windows and OpenVMS), an optional
	/// device name (also only Windows and OpenVMS),
	/// a list of directory names and an optional
	/// filename.
{
public:
	enum Style
	{
		PATH_UNIX,    /// Unix-style path
		PATH_WINDOWS, /// Windows-style path
		PATH_VMS,     /// VMS-style path
		PATH_NATIVE,  /// The current platform's native style
		PATH_GUESS    /// Guess the style by examining the path
	};
	
	typedef std::vector<std::string> StringVec;

	Path();
		/// Creates an empty relative path.

	Path(bool absolute);
		/// Creates an empty absolute or relative path.

	Path(const char* path);
		/// Creates a path from a string.

	Path(const char* path, Style style);
		/// Creates a path from a string.

	Path(const std::string& path);
		/// Creates a path from a string.

	Path(const std::string& path, Style style);
		/// Creates a path from a string.

	Path(const Path& path);
		/// Copy constructor

	Path(const Path& parent, const std::string& fileName);
		/// Creates a path from a parent path and a filename.
		/// The parent path is expected to reference a directory.

	Path(const Path& parent, const char* fileName);
		/// Creates a path from a parent path and a filename.
		/// The parent path is expected to reference a directory.

	Path(const Path& parent, const Path& relative);
		/// Creates a path from a parent path and a relative path.
		/// The parent path is expected to reference a directory.
		/// The relative path is appended to the parent path.

	~Path();
		/// Destroys the Path.
		
	Path& operator = (const Path& path);
		/// Assignment operator.
		
	Path& operator = (const std::string& path);
		/// Assigns a string containing a path in native format.

	Path& operator = (const char* path);
		/// Assigns a string containing a path in native format.

	void swap(Path& path);
		/// Swaps the path with another one.

	Path& assign(const std::string& path);
		/// Assigns a string containing a path in native format.
		
	Path& assign(const std::string& path, Style style);
		/// Assigns a string containing a path.

	Path& assign(const Path& path);
		/// Assigns the given path.
		
	Path& assign(const char* path);
		/// Assigns a string containing a path.

	std::string toString() const;
		/// Returns a string containing the path in native format.
		
	std::string toString(Style style) const;
		/// Returns a string containing the path in the given format.
		
	Path& parse(const std::string& path);
		/// Same as assign().

	Path& parse(const std::string& path, Style style);
		/// Assigns a string containing a path.

	bool tryParse(const std::string& path);
		/// Tries to interpret the given string as a path
		/// in native format.
		/// If the path is syntactically valid, assigns the
		/// path and returns true. Otherwise leaves the 
		/// object unchanged and returns false.

	bool tryParse(const std::string& path, Style style);
		/// Tries to interpret the given string as a path,
		/// according to the given style.
		/// If the path is syntactically valid, assigns the
		/// path and returns true. Otherwise leaves the
		/// object unchanged and returns false.

	Path& parseDirectory(const std::string& path);
		/// The resulting path always refers to a directory and
		/// the filename part is empty.

	Path& parseDirectory(const std::string& path, Style style);
		/// The resulting path always refers to a directory and
		/// the filename part is empty.

	Path& makeDirectory();
		/// If the path contains a filename, the filename is appended
		/// to the directory list and cleared. Thus the resulting path
		/// always refers to a directory.

	Path& makeFile();
		/// If the path contains no filename, the last directory
		/// becomes the filename.

	Path& makeParent();
		/// Makes the path refer to its parent.
		
	Path& makeAbsolute();
		/// Makes the path absolute if it is relative.
		/// The current working directory is taken as base directory.

	Path& makeAbsolute(const Path& base);
		/// Makes the path absolute if it is relative.
		/// The given path is taken as base. 

	Path& append(const Path& path);
		/// Appends the given path.
		
	Path& resolve(const Path& path);
		/// Resolves the given path agains the current one.
		///
		/// If the given path is absolute, it replaces the current one.
		/// Otherwise, the relative path is appended to the current path.

	bool isAbsolute() const;
		/// Returns true iff the path is absolute.
		
	bool isRelative() const;
		/// Returns true iff the path is relative.
	
	bool isDirectory() const;
		/// Returns true iff the path references a directory
		/// (the filename part is empty).
		
	bool isFile() const;
		/// Returns true iff the path references a file
		/// (the filename part is not empty).
	
	Path& setNode(const std::string& node);
		/// Sets the node name.
		/// Setting a non-empty node automatically makes
		/// the path an absolute one.
		
	const std::string& getNode() const;
		/// Returns the node name.
		
	Path& setDevice(const std::string& device);
		/// Sets the device name.
		/// Setting a non-empty device automatically makes
		/// the path an absolute one.
		
	const std::string& getDevice() const;
		/// Returns the device name.
	
	int depth() const;
		/// Returns the number of directories in the directory list.

	const std::string& directory(int n) const;
		/// Returns the n'th directory in the directory list.
		/// If n == depth(), returns the filename.
		
	const std::string& operator [] (int n) const;
		/// Returns the n'th directory in the directory list.
		/// If n == depth(), returns the filename.
		
	Path& pushDirectory(const std::string& dir);
		/// Adds a directory to the directory list.
		
	Path& popDirectory();
		/// Removes the last directory from the directory list.
		
	Path& popFrontDirectory();
		/// Removes the first directory from the directory list.
		
	Path& setFileName(const std::string& name);
		/// Sets the filename.
		
	const std::string& getFileName() const;
		/// Returns the filename.

	Path& setBaseName(const std::string& name);
		/// Sets the basename part of the filename and
		/// does not change the extension.

	std::string getBaseName() const;
		/// Returns the basename (the filename sans
		/// extension) of the path.

	Path& setExtension(const std::string& extension);
		/// Sets the filename extension.
				
	std::string getExtension() const;
		/// Returns the filename extension.
		
	const std::string& version() const;
		/// Returns the file version. VMS only.
		
	Path& clear();
		/// Clears all components.

	Path parent() const;
		/// Returns a path referring to the path's
		/// directory.
		
	Path absolute() const;
		/// Returns an absolute variant of the path,
		/// taking the current working directory as base.

	Path absolute(const Path& base) const;
		/// Returns an absolute variant of the path,
		/// taking the given path as base.

	static Path forDirectory(const std::string& path);
		/// Creates a path referring to a directory.

	static Path forDirectory(const std::string& path, Style style);
		/// Creates a path referring to a directory.

	static char separator();
		/// Returns the platform's path name separator, which separates
		/// the components (names) in a path. 
		///
		/// On Unix systems, this is the slash '/'. On Windows systems, 
		/// this is the backslash '\'. On OpenVMS systems, this is the
		/// period '.'.
		
	static char pathSeparator();
		/// Returns the platform's path separator, which separates
		/// single paths in a list of paths.
		///
		/// On Unix systems, this is the colon ':'. On Windows systems,
		/// this is the semicolon ';'. On OpenVMS systems, this is the
		/// comma ','.

	static std::string current();
		/// Returns the current working directory.
		
	static std::string home();
		/// Returns the user's home directory.

	static std::string configHome();
		/// Returns the user's config directory.
		///
		/// On Unix systems, this is the '~/.config/'. On Windows systems,
		/// this is '%APPDATA%'.

	static std::string dataHome();
		/// Returns the user's data directory.
		///
		/// On Unix systems, this is the '~/.local/share/'. On Windows systems,
		/// this is '%APPDATA%'.

	static std::string tempHome();
		/// Returns the user's temp directory.
		///
		/// On Unix systems, this is the '~/.local/temp/'.

	static std::string cacheHome();
		/// Returns the user's cache directory.
		///
		/// On Unix systems, this is the '~/.cache/'. On Windows systems,
		/// this is '%APPDATA%'.

	static std::string temp();
		/// Returns the temporary directory.
		
	static std::string config();
		/// Returns the systemwide config directory.
		///
		/// On Unix systems, this is the '/etc/'.
		
	static std::string null();
		/// Returns the name of the null device.
		
	static std::string expand(const std::string& path);
		/// Expands all environment variables contained in the path.
		///
		/// On Unix, a tilde as first character in the path is
		/// replaced with the path to user's home directory.

	static void listRoots(std::vector<std::string>& roots);
		/// Fills the vector with all filesystem roots available on the
		/// system. On Unix, there is exactly one root, "/".
		/// On Windows, the roots are the drive letters.
		/// On OpenVMS, the roots are the mounted disks.
		
	static bool find(StringVec::const_iterator it, StringVec::const_iterator end, const std::string& name, Path& path);
		/// Searches the file with the given name in the locations (paths) specified
		/// by it and end. A relative path may be given in name.
		///
		/// If the file is found in one of the locations, the complete
		/// path of the file is stored in the path given as argument and true is returned. 
		/// Otherwise false is returned and the path argument remains unchanged.

	static bool find(const std::string& pathList, const std::string& name, Path& path);
		/// Searches the file with the given name in the locations (paths) specified
		/// in pathList. The paths in pathList must be delimited by the platform's
		/// path separator (see pathSeparator()). A relative path may be given in name.
		///
		/// If the file is found in one of the locations, the complete
		/// path of the file is stored in the path given as argument and true is returned. 
		/// Otherwise false is returned and the path argument remains unchanged.
		
	static std::string transcode(const std::string& path);
		/// On Windows, if POCO has been compiled with Windows UTF-8 support 
		/// (POCO_WIN32_UTF8), this function converts a string (usually containing a path) 
		/// encoded in UTF-8 into a string encoded in the current Windows code page.
		/// 
		/// This function should be used for every string passed as a file name to
		/// a string stream or fopen().
		///
		/// On all other platforms, or if POCO has not been compiled with Windows UTF-8
		/// support, this function returns the string unchanged.

protected:
	void parseUnix(const std::string& path);
	void parseWindows(const std::string& path);
	void parseVMS(const std::string& path);
	void parseGuess(const std::string& path);
	std::string buildUnix() const;
	std::string buildWindows() const;
	std::string buildVMS() const;

private:
	std::string _node;
	std::string _device;
	std::string _name;
	std::string _version;
	StringVec   _dirs;
	bool        _absolute;
};


//
// inlines
//
inline bool Path::isAbsolute() const
{
	return _absolute;
}

	
inline bool Path::isRelative() const
{
	return !_absolute;
}


inline bool Path::isDirectory() const
{
	return _name.empty();
}


inline bool Path::isFile() const
{
	return !_name.empty();
}


inline Path& Path::parse(const std::string& path)
{
	return assign(path);
}


inline Path& Path::parse(const std::string& path, Style style)
{
	return assign(path, style);
}


inline const std::string& Path::getNode() const
{
	return _node;
}


inline const std::string& Path::getDevice() const
{
	return _device;
}


inline const std::string& Path::getFileName() const
{
	return _name;
}


inline int Path::depth() const
{
	return int(_dirs.size());
}


inline const std::string& Path::version() const
{
	return _version;
}


inline Path Path::forDirectory(const std::string& path)
{
	Path p;
	return p.parseDirectory(path);
}
	

inline Path Path::forDirectory(const std::string& path, Style style)
{
	Path p;
	return p.parseDirectory(path, style);
}


inline char Path::separator()
{
#if defined(POCO_OS_FAMILY_VMS)
	return '.';
#elif defined(POCO_OS_FAMILY_WINDOWS)
	return '\\';
#else
	return '/';
#endif
}


inline char Path::pathSeparator()
{
#if defined(POCO_OS_FAMILY_VMS)
	return ',';
#elif defined(POCO_OS_FAMILY_WINDOWS)
	return ';';
#else
	return ':';
#endif
}


inline void swap(Path& p1, Path& p2)
{
	p1.swap(p2);
}


} // namespace Poco


#endif // Foundation_Path_INCLUDED
