//
// Document.h
//
// Library: PDF
// Package: PDFCore
// Module:  Document
//
// Definition of the Document class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_Document_INCLUDED
#define PDF_Document_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Page.h"
#include "Poco/PDF/Outline.h"
#include "Poco/PDF/Resource.h"
#include <deque>
#include <map>


namespace Poco {


class LocalDateTime;


namespace PDF {


class PDF_API Document
	/// A Document represents a PDF document object.
{
public:
	typedef HPDF_BYTE*                     DataPtr;
	typedef HPDF_UINT32                    SizeType;
	typedef std::deque<Page>               PageContainer;
	typedef std::deque<Outline>            OutlineContainer;
	typedef std::map<std::string, Font>    FontContainer;
	typedef std::map<std::string, Encoder> EncoderContainer;
	typedef std::map<std::string, Image>   ImageContainer;

	enum Info
	{
		INFO_CREATION_DATE = HPDF_INFO_CREATION_DATE,
		INFO_MOD_DATE = HPDF_INFO_MOD_DATE,
		INFO_AUTHOR = HPDF_INFO_AUTHOR,
		INFO_CREATOR = HPDF_INFO_CREATOR,
		INFO_TITLE = HPDF_INFO_TITLE,
		INFO_SUBJECT = HPDF_INFO_SUBJECT,
		INFO_KEYWORDS = HPDF_INFO_KEYWORDS
	};

	enum Permission
	{
		ENABLE_READ = HPDF_ENABLE_READ,
			/// User can read the document.
		ENABLE_PRINT = HPDF_ENABLE_PRINT,
			/// User can print the document.
		ENABLE_EDIT_ALL = HPDF_ENABLE_EDIT_ALL,
			/// User can edit the contents of the document other than annotations, form fields.
		ENABLE_COPY = HPDF_ENABLE_COPY,
			/// User can copy the text and the graphics of the document.
		ENABLE_EDIT = HPDF_ENABLE_EDIT
			/// User can add or modify the annotations and form fields of the document.
	};

	enum PageLayout
	{
		PAGE_LAYOUT_SINGLE = HPDF_PAGE_LAYOUT_SINGLE, 
			/// Only one page is displayed.
		PAGE_LAYOUT_ONE_COLUMN = HPDF_PAGE_LAYOUT_ONE_COLUMN,
			/// Display the pages in one column.
		PAGE_LAYOUT_TWO_COLUMN_LEFT = HPDF_PAGE_LAYOUT_TWO_COLUMN_LEFT,
			/// Display the pages in two column. The page of the odd number is displayed left. 
		PAGE_LAYOUT_TWO_COLUMN_RIGHT = HPDF_PAGE_LAYOUT_TWO_COLUMN_RIGHT
			/// Display the pages in two column. The page of the odd number is displayed right.  
	};

	enum PageMode
	{
		PAGE_MODE_USE_NONE = HPDF_PAGE_MODE_USE_NONE, 
			/// Display the document with neither outline nor thumbnail.  
		PAGE_MODE_USE_OUTLINE = HPDF_PAGE_MODE_USE_OUTLINE,
			/// Display the document with outline pain.
		PAGE_MODE_USE_THUMBS = HPDF_PAGE_MODE_USE_THUMBS,
			///Display the document with thumbnail pain.
		PAGE_MODE_FULL_SCREEN = HPDF_PAGE_MODE_FULL_SCREEN
			/// Display the document with full screen mode. 
	};

	enum Compression
	{
		COMPRESSION_NONE = HPDF_COMP_NONE,
			/// All contents are not compressed.
		COMPRESSION_TEXT = HPDF_COMP_TEXT,
			/// Compress the contents stream of the page.  
		COMPRESSION_IMAGE = HPDF_COMP_IMAGE,
			/// Compress the streams of the image objects. 
		COMPRESSION_METADATA = HPDF_COMP_METADATA,
			/// Other stream datas (fonts, cmaps and so on)  are compressed.
		COMPRESSION_ALL = HPDF_COMP_ALL 	
			/// All stream datas are compressed. 
			/// (Same as HPDF_COMP_TEXT | HPDF_COMP_IMAGE | HPDF_COMP_METADATA)
	};

	enum Encryption
	{
		ENCRYPT_R2 = HPDF_ENCRYPT_R2,
			/// Use "Revision 2" algorithm.
			/// The length of key is automatically set to 5(40bit).
		ENCRYPT_R3 = HPDF_ENCRYPT_R3
			/// Use "Revision 3" algorithm.
			/// Between 5(40bit) and 16(128bit) can be specified for length of the key. 
	};

	enum PageNumberStyle
	{
		PAGE_NUM_STYLE_DECIMAL = HPDF_PAGE_NUM_STYLE_DECIMAL,
			/// Page label is displayed by Arabic numerals.
		PAGE_NUM_STYLE_UPPER_ROMAN = HPDF_PAGE_NUM_STYLE_UPPER_ROMAN,
			/// Page label is displayed by Uppercase roman numerals.
		PAGE_NUM_STYLE_LOWER_ROMAN = HPDF_PAGE_NUM_STYLE_LOWER_ROMAN,
			/// Page label is displayed by Lowercase roman numerals.
		PAGE_NUM_STYLE_UPPER_LETTERS = HPDF_PAGE_NUM_STYLE_UPPER_LETTERS,
			/// Page label is displayed by Uppercase letters (using A to Z).
		PAGE_NUM_STYLE_LOWER_LETTERS = HPDF_PAGE_NUM_STYLE_LOWER_LETTERS,
			/// Page label is displayed by Lowercase letters (using a to z).
	};

	Document(const std::string fileName = "",
		Poco::UInt32 pageCount = 1,
		Page::Size pageSize = Page::PAGE_SIZE_LETTER,
		Page::Orientation orientation = Page::ORIENTATION_PORTRAIT);
		/// Creates the Document, sets the file name to the specified value and
		/// creates the specified number of pages.

	Document(Poco::UInt32 pageCount,
		Page::Size pageSize = Page::PAGE_SIZE_LETTER,
		Page::Orientation orientation = Page::ORIENTATION_PORTRAIT);
		/// Creates the Document, sets the file name to the specified value and
		/// creates the specified number of pages.

	virtual ~Document();
		/// Destroys the Document.

	void createNew(bool resetAll = false);
		/// Resets the current document and creates a new one.
		/// If resetAll is true, the loaded resources are unloaded
		/// prior to creating the new document.

	void save(const std::string fileName = "");
		/// Saves the document to the specified file.
		/// If fileName is empty string, the member variable
		/// _fileName is used. If member variable is empty string,
		/// document is saved to the memory stream.

	const DataPtr data(SizeType& sz);
		/// Returns the document content as raw data and data size in
		/// the sz argument.

	SizeType size();
		/// Resets the document stream, reads the document into the stream and 
		/// returns the document data size.

	void setPages(std::size_t pagePerPages);
		/// Sets the number of pages per page. 
		/// See HARU library HPDF_SetPagesConfiguration API call
		/// documentation for detailed explanation.

	void setPageLayout(PageLayout pageLayout);
		/// Sets the page layout.

	PageLayout getPageLayout() const;
		/// Returns the current page layout.

	void setPageMode(PageMode pageMode);
		/// Sets the page mode.

	PageMode getPageMode() const;
		/// Returns the current page mode.

	const Page& getPage(int index);
		/// Returns the page at index position.

	const Page& operator [] (int index);
		/// Returns the page at index position.

	const Page& getCurrentPage();
		/// Returns the current page.

	const Page& addPage(Page::Size pageSize= Page::PAGE_SIZE_LETTER,
		Page::Orientation orientation = Page::ORIENTATION_PORTRAIT);
		/// Adds a page at the end of the document and returns the reference to it.

	const Page& insertPage(int index,
		Page::Size pageSize= Page::PAGE_SIZE_LETTER,
		Page::Orientation orientation = Page::ORIENTATION_PORTRAIT);
		/// Inserts the page before the page at index position and returns the reference to it.

	//void openAction();
		/// Sets the first page that appears when document is opened.

	const Font& loadFont(const std::string& name, const std::string& encoding);

	const Font& font(const std::string& name, const std::string& encoding = "");
		/// Looks for the font with specified name in the font container.
		/// If the font is not found, it is created.
		/// Returns the reference to the requested font.

	std::string loadType1Font(const std::string& afmFileName, const std::string& pfmFileName);
		/// Loads type 1 font from file. Returns font name.

	std::string loadTTFont(const std::string& fileName, bool embed, int index = -1);
		/// Loads true type font from file. Returns font name.
		/// If the embed parameter is true, the glyph data of the font is embedded, 
		/// otherwise only the matrix data is included in PDF file.

	const Image& loadPNGImage(const std::string& fileName);
		/// Loads the specified PNG image from the file and returns reference to it.

	const Image& loadPNGImageInfo(const std::string& fileName);
		/// Loads the specified PNG image information from the file and returns reference to it.
		/// Unlike loadPNGImage, this function does not load the whole data immediately. 
		/// Only size and color properties are loaded. The image data is loaded just before the 
		/// image object is written to PDF, and the loaded data is deleted immediately. 

	const Image& loadJPEGImage(const std::string& fileName);
		/// Loads the specified PNG image from the file and returns reference to it.

	void compression(Compression mode);
		/// Sets the compression mode.

	void encryption(Encryption mode, Poco::UInt32 keyLength);
		/// Sets the encryption mode.

	const Encoder& loadEncoder(const std::string& name);
		/// Loads the encoder.

	const Encoder& getCurrentEncoder();
		/// Returns the current encoder.

	const Encoder& setCurrentEncoder(const std::string& name);
		/// Set the encoder as current and returns a reference to it.

	void addPageLabel(int pageNum, PageNumberStyle style, int firstPage, const std::string& prefix = "");
		/// adds page labeling range for the document.

	void useUTF8Encoding();
		/// Enables use of UTF-8 encoding (default enabled).

	void useJapaneseFonts();
		/// Enables use of Japanese fonts.

	void useKoreanFonts();
		/// Enables use of Korean fonts.

	void useChineseFonts();
		/// Enables use of Chinese fonts.

	void useChineseTraditionalFonts();
		/// Enables use of Chinese Traditional fonts.

	void useJapaneseEncodings();
		/// Enables use of Japanese encodings.

	void useKoreanEncodings();
		/// Enables use of Korean encodings.

	void useChineseEncodings();
		/// Enables use of Chinese encodings.

	void useChineseTraditionalEncodings();
		/// Enables use of Chinese Traditional encodings.

	void extendedGraphicState();
		/// Creates extended graphic state object.
		/// Bumps up the version of PDF to 1.4.
		/// NOTE:
		/// In Acrobat Reader 5.0, when ExtGState object is used combined with HPDF_Page_Concat(), 
		/// there is a case that cannot be correctly displayed.

	const Outline& createOutline(const std::string& title, const Outline& outline, const Encoder& encoder);
		/// Creates the outline.

	void setInfo(Info info, const std::string& value);
		/// Sets the document info.
 
	void setInfo(Info info, const LocalDateTime& dt);
		/// Sets the document creation or moidification date.

	std::string getInfo(Info info);
		/// Returns the document info.

	void setPassword(const std::string& ownerPassword, const std::string& userPassword);
		/// Sets the document owner and user passwords.

	void setPermission(Permission perm);
		/// Sets the permission on the document.

	std::size_t pageCount() const;
		/// Returns number of pages in the document.

private:
	HPDF_Doc& handle();

	void init(Poco::UInt32 pageCount,
		Page::Size pageSize, Page::Orientation orientation);

	void reset(bool all = false);
		/// Resets the current document. If all is true, the loaded
		/// resources (e.g. fonts, encodings ...)are unloaded. Otherwise
		/// the resources are not unloaded.

	const Image& loadPNGImageImpl(const std::string& fileName, bool doLoad);

	HPDF_Doc         _pdf;
	std::string      _fileName;
	DataPtr          _pRawData;
	SizeType         _size;
	PageContainer    _pages;
	FontContainer    _fonts;
	EncoderContainer _encoders;
	OutlineContainer _outlines;
	ImageContainer   _images;

	friend class Page;
};


//
// inlines
//

inline void Document::setPages(std::size_t pagePerPages)
{
	HPDF_SetPagesConfiguration(_pdf, static_cast<HPDF_UINT>(pagePerPages));
}


inline void Document::setPageLayout(PageLayout pageLayout)
{
	HPDF_SetPageLayout(_pdf, static_cast<HPDF_PageLayout>(pageLayout));
}


inline Document::PageLayout Document::getPageLayout() const
{
	return static_cast<PageLayout>(HPDF_GetPageLayout(_pdf));
}


inline void Document::setPageMode(PageMode pageMode)
{
	HPDF_SetPageMode(_pdf, static_cast<HPDF_PageMode>(pageMode));
}


inline Document::PageMode Document::getPageMode() const
{
	return static_cast<PageMode>(HPDF_GetPageMode(_pdf));
}

/*
inline void openAction()
{
	HPDF_SetOpenAction(_pdf, HPDF_Destination open_action);
}
*/


inline const Page& Document::getPage(int index)
{
	return _pages.at(index);
}


inline const Page& Document::operator [] (int index)
{
	return _pages[index];
}


inline void Document::compression(Compression mode)
{
	HPDF_SetCompressionMode(_pdf, mode);
}


inline void Document::addPageLabel(int pageNum, PageNumberStyle style, int firstPage, const std::string& prefix)
{
	HPDF_AddPageLabel(_pdf,
		static_cast<HPDF_UINT>(pageNum),
		static_cast<HPDF_PageNumStyle>(style),
		static_cast<HPDF_UINT>(firstPage),
		prefix.c_str());
}


inline void Document::useUTF8Encoding()
{
	HPDF_UseUTFEncodings(_pdf);
}


inline void Document::useJapaneseFonts()
{
	HPDF_UseJPFonts(_pdf);
}


inline void Document::useKoreanFonts()
{
	HPDF_UseKRFonts(_pdf);
}


inline void Document::useChineseFonts()
{
	HPDF_UseCNSFonts(_pdf);
}


inline void Document::useChineseTraditionalFonts()
{
	HPDF_UseCNTFonts(_pdf);
}


inline void Document::useJapaneseEncodings()
{
	HPDF_UseJPEncodings(_pdf);
}


inline void Document::useKoreanEncodings()
{
	HPDF_UseKREncodings(_pdf);
}


inline void Document::useChineseEncodings()
{
	HPDF_UseCNSEncodings(_pdf);
}


inline void Document::useChineseTraditionalEncodings()
{
	HPDF_UseCNTEncodings(_pdf);
}


inline void Document::extendedGraphicState()
{
	HPDF_CreateExtGState(_pdf);
}


inline const Image& Document::loadPNGImage(const std::string& fileName)
{
	return loadPNGImageImpl(fileName, true);
}


inline const Image& Document::loadPNGImageInfo(const std::string& fileName)
{
	return loadPNGImageImpl(fileName, false);
}


inline std::string Document::getInfo(Info info)
{
	return HPDF_GetInfoAttr(_pdf, static_cast<HPDF_InfoType>(info));
}


inline void Document::setPermission(Permission perm)
{
	HPDF_SetPermission(_pdf, static_cast<HPDF_UINT>(perm));
}


inline std::size_t Document::pageCount() const
{
	return _pages.size();
}


inline HPDF_Doc& Document::handle()
{
	return _pdf;
}


} } // namespace Poco::PDF


#endif // PDF_Document_INCLUDED
