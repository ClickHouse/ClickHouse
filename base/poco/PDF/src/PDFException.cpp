//
// PDFException.cpp
//
// Library: PDF
// Package: PDFCore
// Module:  PDFException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PDF/PDFException.h"
#include "Poco/Format.h"
#include <string>
#include <typeinfo>


namespace Poco {
namespace PDF {


void HPDF_Error_Handler(HPDF_STATUS error_no, HPDF_STATUS detail_no, void* user_data)
{
	switch (error_no)
	{
	case HPDF_ARRAY_COUNT_ERR: 
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_ARRAY_ITEM_NOT_FOUND: 
		throw NotFoundException("Internal error. The consistency of the data was lost.");
	case HPDF_ARRAY_ITEM_UNEXPECTED_TYPE:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_BINARY_LENGTH_ERR:
		throw InvalidArgumentException("The length of the data exceeds HPDF_LIMIT_MAX_STRING_LEN.");
	case HPDF_CANNOT_GET_PALLET:
		throw NotFoundException("Cannot get a pallet data from PNG image."); 
	case HPDF_DICT_COUNT_ERR:
		throw InvalidArgumentException("The count of elements of a dictionary exceeds HPDF_LIMIT_MAX_DICT_ELEMENT");
	case HPDF_DICT_ITEM_NOT_FOUND:
		throw NotFoundException("Internal error. The consistency of the data was lost."); 
	case HPDF_DICT_ITEM_UNEXPECTED_TYPE:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_DICT_STREAM_LENGTH_NOT_FOUND:
		throw NotFoundException("Internal error. The consistency of the data was lost.");
	case HPDF_DOC_ENCRYPTDICT_NOT_FOUND:
		throw NotFoundException("HPDF_SetPermission() OR HPDF_SetEncryptMode() was called before a password is set.");
	case HPDF_DOC_INVALID_OBJECT:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");  
	case HPDF_DUPLICATE_REGISTRATION:
		throw IllegalStateException("Tried to register a font that has been registered.");
	case HPDF_EXCEED_JWW_CODE_NUM_LIMIT:
		throw IllegalStateException("Cannot register a character to the japanese word wrap characters list.");
	case HPDF_ENCRYPT_INVALID_PASSWORD:
		throw IllegalStateException("Tried to set the owner password to NULL.");
	case HPDF_ERR_UNKNOWN_CLASS:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost."); 
	case HPDF_EXCEED_GSTATE_LIMIT:
		throw IllegalStateException("The depth of the stack exceeded HPDF_LIMIT_MAX_GSTATE.");
	case HPDF_FAILD_TO_ALLOC_MEM:
		throw IllegalStateException("Memory allocation failed.");
	case HPDF_FILE_IO_ERROR:
		throw IOException("File processing failed. (A detailed code is set.)");
	case HPDF_FILE_OPEN_ERROR:
		throw IOException("Cannot open a file. (A detailed code is set.)");
	case HPDF_FONT_EXISTS:
		throw IllegalStateException("Tried to load a font that has been registered."); 
	case HPDF_FONT_INVALID_WIDTHS_TABLE:
		throw IllegalStateException("The format of a font-file is invalid. Internal error. The consistency of the data was lost.");  
	case HPDF_INVALID_AFM_HEADER:
		throw IllegalStateException("Cannot recognize a header of an afm file.");
	case HPDF_INVALID_ANNOTATION:
		throw IllegalStateException("The specified annotation handle is invalid."); 
	case HPDF_INVALID_BIT_PER_COMPONENT:
		throw IllegalStateException("Bit-per-component of a image which was set as mask-image is invalid.");
	case HPDF_INVALID_CHAR_MATRICS_DATA:
		throw IllegalStateException("Cannot recognize char-matrics-data  of an afm file.");
	case HPDF_INVALID_COLOR_SPACE:
		switch (detail_no)
		{
		case 1: 
			throw InvalidArgumentException("The color_space parameter of HPDF_LoadRawImage is invalid.");
		case 2: 
			throw InvalidArgumentException("Color-space of a image which was set as mask-image is invalid.");
		case 3: 
			throw InvalidArgumentException("The function which is invalid in the present color-space was invoked.");
		default:
			throw PDFException();
		}
	case HPDF_INVALID_COMPRESSION_MODE:
		throw InvalidArgumentException("Invalid value was set when invoking HPDF_SetCommpressionMode().");
	case HPDF_INVALID_DATE_TIME:
		throw InvalidArgumentException("An invalid date-time value was set.");
	case HPDF_INVALID_DESTINATION:
		throw InvalidArgumentException("An invalid destination handle was set.");
	case HPDF_INVALID_DOCUMENT:
		throw InvalidArgumentException("An invalid document handle is set.");
	case HPDF_INVALID_DOCUMENT_STATE:
		throw IllegalStateException("The function which is invalid in the present state was invoked.");
	case HPDF_INVALID_ENCODER:
		throw InvalidArgumentException("An invalid encoder handle is set.");
	case HPDF_INVALID_ENCODER_TYPE:
		throw InvalidArgumentException("A combination between font and encoder is wrong.");
	case HPDF_INVALID_ENCODING_NAME:
		throw InvalidArgumentException("An Invalid encoding name is specified.");
	case HPDF_INVALID_ENCRYPT_KEY_LEN:
		throw InvalidArgumentException("The lengh of the key of encryption is invalid.");
	case HPDF_INVALID_FONTDEF_DATA:
		switch (detail_no)
		{
		case 1: 
			throw InvalidArgumentException("An invalid font handle was set.");
		case 2:
			throw InvalidArgumentException("Unsupported font format.");
		default:
			throw PDFException();
		}
	case HPDF_INVALID_FONTDEF_TYPE:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");
	case HPDF_INVALID_FONT_NAME:
		throw NotFoundException("A font which has the specified name is not found.");
	case HPDF_INVALID_IMAGE:
		throw InvalidArgumentException("Unsupported image format.");
	case HPDF_INVALID_JPEG_DATA:
		throw InvalidArgumentException("Unsupported image format.");
	case HPDF_INVALID_N_DATA:
		throw IOException("Cannot read a postscript-name from an afm file.");
	case HPDF_INVALID_OBJECT:
		switch (detail_no)
		{
		case 1:
			throw IllegalStateException("An invalid object is set.");
		case 2: 
			throw IllegalStateException("Internal error. The consistency of the data was lost.");
		default:
			throw PDFException();
		}
	case HPDF_INVALID_OBJ_ID:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_INVALID_OPERATION:
		switch (detail_no)
		{
		case 1:
			throw IllegalStateException("Invoked HPDF_Image_SetColorMask() against the image-object which was set a mask-image.");
		default:
			throw PDFException();
		}
	case HPDF_INVALID_OUTLINE:
		throw InvalidArgumentException("An invalid outline-handle was specified.");
	case HPDF_INVALID_PAGE:
		throw InvalidArgumentException("An invalid page-handle was specified.");
	case HPDF_INVALID_PAGES:
		throw InvalidArgumentException("An invalid pages-handle was specified. (internel error)");
	case HPDF_INVALID_PARAMETER:
		throw InvalidArgumentException("An invalid value is set.");
	case HPDF_INVALID_PNG_IMAGE:
		throw InvalidArgumentException("Invalid PNG image format.");
	case HPDF_INVALID_STREAM:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_MISSING_FILE_NAME_ENTRY:
		throw InvalidArgumentException("Internal error. The \"_FILE_NAME\" entry for delayed loading is missing.");
	case HPDF_INVALID_TTC_FILE:
		throw InvalidArgumentException("Invalid .TTC file format.");
	case HPDF_INVALID_TTC_INDEX:
		throw InvalidArgumentException("The index parameter was exceed the number of included fonts");
	case HPDF_INVALID_WX_DATA:
		throw IOException("Cannot read a width-data from an afm file.");
	case HPDF_ITEM_NOT_FOUND:
		throw NotFoundException("Internal error. The consistency of the data was lost."); 
	case HPDF_LIBPNG_ERROR:
		throw IOException("An error has returned from PNGLIB while loading an image.");
	case HPDF_NAME_INVALID_VALUE:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_NAME_OUT_OF_RANGE:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");  
	case HPDF_PAGES_MISSING_KIDS_ENTRY:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");  
	case HPDF_PAGE_CANNOT_FIND_OBJECT:
		throw NotFoundException("Internal error. The consistency of the data was lost.");
	case HPDF_PAGE_CANNOT_GET_ROOT_PAGES:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_PAGE_CANNOT_RESTORE_GSTATE:
		throw IllegalStateException("There are no graphics-states to be restored.");
	case HPDF_PAGE_CANNOT_SET_PARENT:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");
	case HPDF_PAGE_FONT_NOT_FOUND:
		throw NotFoundException("The current font is not set.");
	case HPDF_PAGE_INVALID_FONT:
		throw InvalidArgumentException("An invalid font-handle was spacified.");
	case HPDF_PAGE_INVALID_FONT_SIZE:
		throw InvalidArgumentException("An invalid font-size was set.");
	case HPDF_PAGE_INVALID_GMODE:
		throw InvalidArgumentException("See Graphics mode.");
	case HPDF_PAGE_INVALID_INDEX:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_PAGE_INVALID_ROTATE_VALUE:
		throw InvalidArgumentException("The specified value is not a multiple of 90.");
	case HPDF_PAGE_INVALID_SIZE:
		throw InvalidArgumentException("An invalid page-size was set.");  
	case HPDF_PAGE_INVALID_XOBJECT:
		throw InvalidArgumentException("An invalid image-handle was set.");
	case HPDF_PAGE_OUT_OF_RANGE:
		throw RangeException("The specified value is out of range.");
	case HPDF_REAL_OUT_OF_RANGE:
		throw RangeException("The specified value is out of range.");
	case HPDF_STREAM_EOF:
		throw IllegalStateException("Unexpected EOF marker was detected.");
	case HPDF_STREAM_READLN_CONTINUE:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");
	case HPDF_STRING_OUT_OF_RANGE:
		throw RangeException("The length of the specified text is too long.");
	case HPDF_THIS_FUNC_WAS_SKIPPED:
		throw IllegalStateException("The execution of a function was skipped because of other errors.");
	case HPDF_TTF_CANNOT_EMBEDDING_FONT:
		throw IllegalStateException("This font cannot be embedded. (restricted by license)");
	case HPDF_TTF_INVALID_CMAP:
		throw InvalidArgumentException("Unsupported ttf format. (cannot find unicode cmap.)");
	case HPDF_TTF_INVALID_FOMAT:
		throw InvalidArgumentException("Unsupported ttf format.");
	case HPDF_TTF_MISSING_TABLE:
		throw InvalidArgumentException("Unsupported ttf format. (cannot find a necessary table)");  
	case HPDF_UNSUPPORTED_FONT_TYPE:
		throw InvalidArgumentException("Internal error. The consistency of the data was lost.");
	case HPDF_UNSUPPORTED_FUNC:
		switch (detail_no)
		{
		case 1:
			throw IllegalStateException("The library is not configured to use PNGLIB.");
		case 2:
			throw IllegalStateException("Internal error. The consistency of the data was lost.");
		default:
			throw PDFException();
		}
	case HPDF_UNSUPPORTED_JPEG_FORMAT:
		throw InvalidArgumentException("Unsupported JPEG format.");
	case HPDF_UNSUPPORTED_TYPE1_FONT:
		throw IllegalStateException("Failed to parse .PFB file.");
	case HPDF_XREF_COUNT_ERR:
		throw IllegalStateException("Internal error. The consistency of the data was lost.");
	case HPDF_ZLIB_ERROR:
		throw IllegalStateException("An error has occurred while executing a function of Zlib.");
	case HPDF_INVALID_PAGE_INDEX:
		throw IllegalStateException("An error returned from Zlib.");
	case HPDF_INVALID_URI:
		throw InvalidArgumentException("An invalid URI was set.");
	case HPDF_PAGE_LAYOUT_OUT_OF_RANGE:
		throw RangeException("An invalid page-layout was set.");
	case HPDF_PAGE_MODE_OUT_OF_RANGE:
		throw RangeException("An invalid page-mode was set.");
	case HPDF_PAGE_NUM_STYLE_OUT_OF_RANGE:
		throw RangeException("An invalid page-num-style was set.");
	case HPDF_ANNOT_INVALID_ICON:
		throw InvalidArgumentException("An invalid icon was set.");
	case HPDF_ANNOT_INVALID_BORDER_STYLE:
		throw InvalidArgumentException("An invalid border-style was set.");
	case HPDF_PAGE_INVALID_DIRECTION:
		throw InvalidArgumentException("An invalid page-direction was set.");
	case HPDF_INVALID_FONT:
		throw InvalidArgumentException("An invalid font-handle was specified. ");
	default:
		throw PDFException();
	}
}


POCO_IMPLEMENT_EXCEPTION(PDFException, Poco::RuntimeException, "PDF Base Exception")
POCO_IMPLEMENT_EXCEPTION(PDFCreateException, PDFException, "PDF creation failed")


} } // namespace Poco::PDF
