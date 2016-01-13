//
// Decompress.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/Decompress.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  Decompress
//
// Definition of the Decompress class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Decompress_INCLUDED
#define Zip_Decompress_INCLUDED


#include "Poco/Zip/Zip.h"
#include "Poco/Zip/ParseCallback.h"
#include "Poco/Zip/ZipArchive.h"
#include "Poco/Path.h"
#include "Poco/FIFOEvent.h"


namespace Poco {
namespace Zip {


class ZipArchive;


class Zip_API Decompress: public ParseCallback
	/// Decompress extracts files from zip files, can be used to extract single files or all files
{
public:
	typedef std::map<std::string, Poco::Path> ZipMapping;
		/// Maps key of FileInfo entries to their local decompressed representation
	Poco::FIFOEvent<std::pair<const ZipLocalFileHeader, const std::string> > EError;
		/// Thrown whenever an error is detected when handling a ZipLocalFileHeader entry. The string contains an error message
	Poco::FIFOEvent<std::pair<const ZipLocalFileHeader, const Poco::Path> > EOk;
		/// Thrown whenever a file was successfully decompressed

	Decompress(std::istream& in, const Poco::Path& outputDir, bool flattenDirs = false, bool keepIncompleteFiles = false);
		/// Creates the Decompress. Note that istream must be good and at the very beginning of the file!
		/// Calling decompressAllFiles will cause the stream to be in state failed once the zip file is processed.
		/// outputDir must be a directory. If it doesn't exist yet, it will be automatically created.
		/// If flattenDirs is set to true, the directory structure of the zip file is not recreated. 
		/// Instead, all files are extracted into one single directory.

	~Decompress();
		/// Destroys the Decompress.

	ZipArchive decompressAllFiles();
		/// Decompresses all files stored in the zip File. Can only be called once per Decompress object.
		/// Use mapping to retrieve the location of the decompressed files

	bool handleZipEntry(std::istream& zipStream, const ZipLocalFileHeader& hdr);

	const ZipMapping& mapping() const;
		/// A ZipMapping stores as key the full name of the ZipFileInfo/ZipLocalFileHeader and as value the decompressed file
		/// If for a ZipFileInfo no mapping exists, there was an error during decompression and the entry is considered to be corrupt

private:
	Decompress(const Decompress&);
	Decompress& operator=(const Decompress&);

	void onOk(const void*, std::pair<const ZipLocalFileHeader, const Poco::Path>& val);

private:
	std::istream& _in;
	Poco::Path    _outDir;
	bool          _flattenDirs;
	bool          _keepIncompleteFiles;
	ZipMapping    _mapping;
};


inline const Decompress::ZipMapping& Decompress::mapping() const
{
	return _mapping;
}


} } // namespace Poco::Zip


#endif // Zip_Decompress_INCLUDED
