/*
 * << Haru Free PDF Library >> -- hpdf_image.c
 *
 * URL: http://libharu.org
 *
 * Copyright (c) 1999-2006 Takeshi Kanno <takeshi_kanno@est.hi-ho.ne.jp>
 * Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>
 *
 * Permission to use, copy, modify, distribute and sell this software
 * and its documentation for any purpose is hereby granted without fee,
 * provided that the above copyright notice appear in all copies and
 * that both that copyright notice and this permission notice appear
 * in supporting documentation.
 * It is provided "as is" without express or implied warranty.
 *
 */

#include "hpdf_conf.h"
#include "hpdf_utils.h"
#include "hpdf.h"

static const char *COL_CMYK = "DeviceCMYK";
static const char *COL_RGB = "DeviceRGB";
static const char *COL_GRAY = "DeviceGray";

static HPDF_STATUS
LoadJpegHeader (HPDF_Image   image,
                HPDF_Stream  stream);


/*---------------------------------------------------------------------------*/

static HPDF_STATUS
LoadJpegHeader (HPDF_Image   image,
                HPDF_Stream  stream)
{
    HPDF_UINT16 tag;
    HPDF_UINT16 height;
    HPDF_UINT16 width;
    HPDF_BYTE precision;
    HPDF_BYTE num_components;
    const char *color_space_name;
    HPDF_UINT len;
    HPDF_STATUS ret;
    HPDF_Array array;

    HPDF_PTRACE ((" HPDF_Image_LoadJpegHeader\n"));

    len = 2;
    if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&tag, &len) != HPDF_OK)
        return HPDF_Error_GetCode (stream->error);

    HPDF_UInt16Swap (&tag);
    if (tag != 0xFFD8)
        return HPDF_INVALID_JPEG_DATA;

    /* find SOF record */
    for (;;) {
        HPDF_UINT16 size;

        len = 2;
        if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&tag,  &len) != HPDF_OK)
            return HPDF_Error_GetCode (stream->error);

        HPDF_UInt16Swap (&tag);

        len = 2;
        if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&size,  &len) != HPDF_OK)
            return HPDF_Error_GetCode (stream->error);

        HPDF_UInt16Swap (&size);

        HPDF_PTRACE (("tag=%04X size=%u\n", tag, size));

        if (tag == 0xFFC0 || tag == 0xFFC1 ||
                tag == 0xFFC2 || tag == 0xFFC9) {

            len = 1;
            if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&precision, &len) !=
                    HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

            len = 2;
            if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&height, &len) !=
                    HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

            HPDF_UInt16Swap (&height);

            len = 2;
            if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&width, &len) != HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

             HPDF_UInt16Swap (&width);

           len = 1;
            if (HPDF_Stream_Read (stream, (HPDF_BYTE *)&num_components, &len) !=
                    HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

            break;
        } else if ((tag | 0x00FF) != 0xFFFF)
            /* lost marker */
            return HPDF_SetError (image->error, HPDF_UNSUPPORTED_JPEG_FORMAT,
                    0);

        if (HPDF_Stream_Seek (stream, size - 2, HPDF_SEEK_CUR) != HPDF_OK)
                return HPDF_Error_GetCode (stream->error);
    }

    if (HPDF_Dict_AddNumber (image, "Height", height) != HPDF_OK)
        return HPDF_Error_GetCode (stream->error);

    if (HPDF_Dict_AddNumber (image, "Width", width) != HPDF_OK)
        return HPDF_Error_GetCode (stream->error);

    /* classification of RGB and CMYK is less than perfect
     * YCbCr and YCCK are classified into RGB or CMYK.
     *
     * It is necessary to read APP14 data to distinguish colorspace perfectly.

     */
    switch (num_components) {
        case 1:
            color_space_name = COL_GRAY;
            break;
        case 3:
            color_space_name = COL_RGB;
            break;
        case 4:
            array = HPDF_Array_New (image->mmgr);
            if (!array)
                return HPDF_Error_GetCode (stream->error);

            ret = HPDF_Dict_Add (image, "Decode", array);
            if (ret != HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 1));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 0));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 1));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 0));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 1));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 0));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 1));
            ret += HPDF_Array_Add (array, HPDF_Number_New (image->mmgr, 0));

            if (ret != HPDF_OK)
                return HPDF_Error_GetCode (stream->error);

            color_space_name = COL_CMYK;

            break;
        default:
            return HPDF_SetError (image->error, HPDF_UNSUPPORTED_JPEG_FORMAT,
                    0);
    }

    if (HPDF_Dict_Add (image, "ColorSpace",
                HPDF_Name_New (image->mmgr, color_space_name)) != HPDF_OK)
        return HPDF_Error_GetCode (stream->error);

    if (HPDF_Dict_Add (image, "BitsPerComponent",
                HPDF_Number_New (image->mmgr, precision)) != HPDF_OK)
        return HPDF_Error_GetCode (stream->error);

    return HPDF_OK;
}

HPDF_Image
HPDF_Image_LoadJpegImage  (HPDF_MMgr        mmgr,
                           HPDF_Stream      jpeg_data,
                           HPDF_Xref        xref)
{
    HPDF_Dict image;
    HPDF_STATUS ret = HPDF_OK;

    HPDF_PTRACE ((" HPDF_Image_LoadJpegImage\n"));

    image = HPDF_DictStream_New (mmgr, xref);
    if (!image)
        return NULL;

    image->header.obj_class |= HPDF_OSUBCLASS_XOBJECT;

    /* add requiered elements */
    image->filter = HPDF_STREAM_FILTER_DCT_DECODE;
    ret += HPDF_Dict_AddName (image, "Type", "XObject");
    ret += HPDF_Dict_AddName (image, "Subtype", "Image");
    if (ret != HPDF_OK)
        return NULL;

    if (LoadJpegHeader (image, jpeg_data) != HPDF_OK)
        return NULL;

    if (HPDF_Stream_Seek (jpeg_data, 0, HPDF_SEEK_SET) != HPDF_OK)
        return NULL;

    for (;;) {
        HPDF_BYTE buf[HPDF_STREAM_BUF_SIZ];
        HPDF_UINT len = HPDF_STREAM_BUF_SIZ;
        HPDF_STATUS ret = HPDF_Stream_Read (jpeg_data, buf,
                &len);

        if (ret != HPDF_OK) {
            if (ret == HPDF_STREAM_EOF) {
                if (len > 0) {
                    ret = HPDF_Stream_Write (image->stream, buf, len);
                    if (ret != HPDF_OK)
                        return NULL;
                }
                break;
            } else
                return NULL;
        }

        if (HPDF_Stream_Write (image->stream, buf, len) != HPDF_OK)
            return NULL;
    }

    return image;
}

HPDF_Image
HPDF_Image_LoadJpegImageFromMem  (HPDF_MMgr    mmgr,
                            const HPDF_BYTE   *buf,
                                  HPDF_UINT    size,
                                  HPDF_Xref    xref)
{
	HPDF_Stream jpeg_data;
	HPDF_Image image;

	HPDF_PTRACE ((" HPDF_Image_LoadJpegImageFromMem\n"));

	jpeg_data = HPDF_MemStream_New(mmgr,size);
	if (!HPDF_Stream_Validate (jpeg_data)) {
		HPDF_RaiseError (mmgr->error, HPDF_INVALID_STREAM, 0);
		return NULL;
	}

	if (HPDF_Stream_Write (jpeg_data, buf, size) != HPDF_OK) {
		HPDF_Stream_Free (jpeg_data);
		return NULL;
	}

	image = HPDF_Image_LoadJpegImage(mmgr,jpeg_data,xref);

	/* destroy file stream */
	HPDF_Stream_Free (jpeg_data);

	return image;
}


HPDF_Image
HPDF_Image_LoadRawImage (HPDF_MMgr          mmgr,
                         HPDF_Stream        raw_data,
                         HPDF_Xref          xref,
                         HPDF_UINT          width,
                         HPDF_UINT          height,
                         HPDF_ColorSpace    color_space)
{
    HPDF_Dict image;
    HPDF_STATUS ret = HPDF_OK;
    HPDF_UINT size;

    HPDF_PTRACE ((" HPDF_Image_LoadRawImage\n"));

    if (color_space != HPDF_CS_DEVICE_GRAY &&
            color_space != HPDF_CS_DEVICE_RGB &&
            color_space != HPDF_CS_DEVICE_CMYK) {
        HPDF_SetError (mmgr->error, HPDF_INVALID_COLOR_SPACE, 0);
        return NULL;
    }

    image = HPDF_DictStream_New (mmgr, xref);
    if (!image)
        return NULL;

    image->header.obj_class |= HPDF_OSUBCLASS_XOBJECT;
    ret += HPDF_Dict_AddName (image, "Type", "XObject");
    ret += HPDF_Dict_AddName (image, "Subtype", "Image");
    if (ret != HPDF_OK)
        return NULL;

    if (color_space == HPDF_CS_DEVICE_GRAY) {
        size = width * height;
        ret = HPDF_Dict_AddName (image, "ColorSpace", COL_GRAY);
	} else if (color_space == HPDF_CS_DEVICE_CMYK) {
		size = width * height * 4;
		ret = HPDF_Dict_AddName (image, "ColorSpace", COL_CMYK);
    } else {
        size = width * height * 3;
        ret = HPDF_Dict_AddName (image, "ColorSpace", COL_RGB);
    }

    if (ret != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Width", width) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Height", height) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "BitsPerComponent", 8) != HPDF_OK)
        return NULL;

    if (HPDF_Stream_WriteToStream (raw_data, image->stream, 0, NULL) != HPDF_OK)
        return NULL;

    if (image->stream->size != size) {
        HPDF_SetError (image->error, HPDF_INVALID_IMAGE, 0);
        return NULL;
    }

    return image;
}


HPDF_Image
HPDF_Image_LoadRawImageFromMem  (HPDF_MMgr          mmgr,
                                 const HPDF_BYTE   *buf,
                                 HPDF_Xref          xref,
                                 HPDF_UINT          width,
                                 HPDF_UINT          height,
                                 HPDF_ColorSpace    color_space,
                                 HPDF_UINT          bits_per_component)
{
    HPDF_Dict image;
    HPDF_STATUS ret = HPDF_OK;
    HPDF_UINT size=0;

    HPDF_PTRACE ((" HPDF_Image_LoadRawImageFromMem\n"));

    if (color_space != HPDF_CS_DEVICE_GRAY &&
            color_space != HPDF_CS_DEVICE_RGB &&
            color_space != HPDF_CS_DEVICE_CMYK) {
        HPDF_SetError (mmgr->error, HPDF_INVALID_COLOR_SPACE, 0);
        return NULL;
    }

    if (bits_per_component != 1 && bits_per_component != 2 &&
            bits_per_component != 4 && bits_per_component != 8) {
        HPDF_SetError (mmgr->error, HPDF_INVALID_IMAGE, 0);
        return NULL;
    }

    image = HPDF_DictStream_New (mmgr, xref);
    if (!image)
        return NULL;

    image->header.obj_class |= HPDF_OSUBCLASS_XOBJECT;
    ret += HPDF_Dict_AddName (image, "Type", "XObject");
    ret += HPDF_Dict_AddName (image, "Subtype", "Image");
    if (ret != HPDF_OK)
        return NULL;

    switch (color_space) {
        case HPDF_CS_DEVICE_GRAY:
            size = (HPDF_UINT)((HPDF_DOUBLE)width * height / (8 / bits_per_component) + 0.876);
            ret = HPDF_Dict_AddName (image, "ColorSpace", COL_GRAY);
            break;
        case HPDF_CS_DEVICE_RGB:
            size = (HPDF_UINT)((HPDF_DOUBLE)width * height / (8 / bits_per_component) + 0.876);
            size *= 3;
            ret = HPDF_Dict_AddName (image, "ColorSpace", COL_RGB);
            break;
        case HPDF_CS_DEVICE_CMYK:
            size = (HPDF_UINT)((HPDF_DOUBLE)width * height / (8 / bits_per_component) + 0.876);
            size *= 4;
            ret = HPDF_Dict_AddName (image, "ColorSpace", COL_CMYK);
            break;
        default:;
    }

    if (ret != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Width", width) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "Height", height) != HPDF_OK)
        return NULL;

    if (HPDF_Dict_AddNumber (image, "BitsPerComponent", bits_per_component)
            != HPDF_OK)
        return NULL;

    if (HPDF_Stream_Write (image->stream, buf, size) != HPDF_OK)
        return NULL;

    return image;
}


HPDF_BOOL
HPDF_Image_Validate (HPDF_Image  image)
{
    HPDF_Name subtype;

    HPDF_PTRACE ((" HPDF_Image_Validate\n"));

    if (!image)
        return HPDF_FALSE;

    if (image->header.obj_class != (HPDF_OSUBCLASS_XOBJECT |
                HPDF_OCLASS_DICT)) {
        HPDF_RaiseError (image->error, HPDF_INVALID_IMAGE, 0);
        return HPDF_FALSE;
    }

    subtype = HPDF_Dict_GetItem (image, "Subtype", HPDF_OCLASS_NAME);
    if (!subtype || HPDF_StrCmp (subtype->value, "Image") != 0) {
        HPDF_RaiseError (image->error, HPDF_INVALID_IMAGE, 0);
        return HPDF_FALSE;
    }

    return HPDF_TRUE;
}


HPDF_EXPORT(HPDF_Point)
HPDF_Image_GetSize (HPDF_Image  image)
{
    HPDF_Number width;
    HPDF_Number height;
    HPDF_Point ret = {0, 0};

    HPDF_PTRACE ((" HPDF_Image_GetSize\n"));

    if (!HPDF_Image_Validate (image))
        return ret;

    width = HPDF_Dict_GetItem (image, "Width", HPDF_OCLASS_NUMBER);
    height = HPDF_Dict_GetItem (image, "Height", HPDF_OCLASS_NUMBER);

    if (width && height) {
      ret.x = (HPDF_REAL)width->value;
      ret.y = (HPDF_REAL)height->value;
    }

    return ret;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Image_GetSize2 (HPDF_Image  image, HPDF_Point *size)
{
    HPDF_Number width;
    HPDF_Number height;
    size->x = 0;
    size->y = 0;

    HPDF_PTRACE ((" HPDF_Image_GetSize\n"));

    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    width = HPDF_Dict_GetItem (image, "Width", HPDF_OCLASS_NUMBER);
    height = HPDF_Dict_GetItem (image, "Height", HPDF_OCLASS_NUMBER);

    if (width && height) {
      size->x = (HPDF_REAL)width->value;
      size->y = (HPDF_REAL)height->value;
    }

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_UINT)
HPDF_Image_GetBitsPerComponent (HPDF_Image  image)
{
    HPDF_Number n;

    HPDF_PTRACE ((" HPDF_Image_GetBitsPerComponent\n"));

    if (!HPDF_Image_Validate (image))
        return 0;

    n = HPDF_Dict_GetItem (image, "BitsPerComponent", HPDF_OCLASS_NUMBER);

    if (!n)
        return 0;

    return n->value;
}

HPDF_EXPORT(const char*)
HPDF_Image_GetColorSpace (HPDF_Image  image)
{
	HPDF_Name n;

	HPDF_PTRACE ((" HPDF_Image_GetColorSpace\n"));

	n = HPDF_Dict_GetItem (image, "ColorSpace", HPDF_OCLASS_NAME);

	if (!n) {
		HPDF_Array a;

		HPDF_Error_Reset(image->error);

		a = HPDF_Dict_GetItem (image, "ColorSpace", HPDF_OCLASS_ARRAY);

		if (a) {
			n = HPDF_Array_GetItem (a, 0, HPDF_OCLASS_NAME);
		}
	}

	if (!n) {
		HPDF_CheckError (image->error);
		return NULL;
	}

	return n->value;
}

HPDF_EXPORT(HPDF_UINT)
HPDF_Image_GetWidth  (HPDF_Image   image)
{
    return (HPDF_UINT)HPDF_Image_GetSize (image).x;
}

HPDF_EXPORT(HPDF_UINT)
HPDF_Image_GetHeight  (HPDF_Image   image)
{
    return (HPDF_UINT)HPDF_Image_GetSize (image).y;
}

HPDF_STATUS
HPDF_Image_SetMask (HPDF_Image   image,
                    HPDF_BOOL    mask)
{
    HPDF_Boolean image_mask;

    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    if (mask && HPDF_Image_GetBitsPerComponent (image) != 1)
        return HPDF_SetError (image->error, HPDF_INVALID_BIT_PER_COMPONENT,
                0);

    image_mask = HPDF_Dict_GetItem (image, "ImageMask", HPDF_OCLASS_BOOLEAN);
    if (!image_mask) {
        HPDF_STATUS ret;
        image_mask = HPDF_Boolean_New (image->mmgr, HPDF_FALSE);

        if ((ret = HPDF_Dict_Add (image, "ImageMask", image_mask)) != HPDF_OK)
            return ret;
    }

    image_mask->value = mask;
    return HPDF_OK;
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Image_SetMaskImage  (HPDF_Image   image,
                          HPDF_Image   mask_image)
{
    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    if (!HPDF_Image_Validate (mask_image))
        return HPDF_INVALID_IMAGE;

    if (HPDF_Image_SetMask (mask_image, HPDF_TRUE) != HPDF_OK)
        return HPDF_CheckError (image->error);

    return HPDF_Dict_Add (image, "Mask", mask_image);
}


HPDF_EXPORT(HPDF_STATUS)
HPDF_Image_SetColorMask (HPDF_Image   image,
                         HPDF_UINT    rmin,
                         HPDF_UINT    rmax,
                         HPDF_UINT    gmin,
                         HPDF_UINT    gmax,
                         HPDF_UINT    bmin,
                         HPDF_UINT    bmax)
{
    HPDF_Array array;
    const char *name;
    HPDF_STATUS ret = HPDF_OK;

    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    if (HPDF_Dict_GetItem (image, "ImageMask", HPDF_OCLASS_BOOLEAN))
        return HPDF_RaiseError (image->error, HPDF_INVALID_OPERATION, 0);

    if (HPDF_Image_GetBitsPerComponent (image) != 8)
        return HPDF_RaiseError (image->error, HPDF_INVALID_BIT_PER_COMPONENT,
                0);

    name = HPDF_Image_GetColorSpace (image);
    if (!name || HPDF_StrCmp (COL_RGB, name) != 0)
        return HPDF_RaiseError (image->error, HPDF_INVALID_COLOR_SPACE, 0);

    /* Each integer must be in the range 0 to 2^BitsPerComponent - 1 */
    if (rmax > 255 || gmax > 255 || bmax > 255)
       return HPDF_RaiseError (image->error, HPDF_INVALID_PARAMETER, 0);

    array = HPDF_Array_New (image->mmgr);
    if (!array)
        return HPDF_CheckError (image->error);

    ret += HPDF_Dict_Add (image, "Mask", array);
    ret += HPDF_Array_AddNumber (array, rmin);
    ret += HPDF_Array_AddNumber (array, rmax);
    ret += HPDF_Array_AddNumber (array, gmin);
    ret += HPDF_Array_AddNumber (array, gmax);
    ret += HPDF_Array_AddNumber (array, bmin);
    ret += HPDF_Array_AddNumber (array, bmax);

    if (ret != HPDF_OK)
        return HPDF_CheckError (image->error);

    return HPDF_OK;
}

HPDF_EXPORT(HPDF_STATUS)
HPDF_Image_AddSMask  (HPDF_Image  image,
                      HPDF_Image  smask)
{

   const char *name;

   if (!HPDF_Image_Validate (image))
       return HPDF_INVALID_IMAGE;
   if (!HPDF_Image_Validate (smask))
       return HPDF_INVALID_IMAGE;

   if (HPDF_Dict_GetItem (image, "SMask", HPDF_OCLASS_BOOLEAN))
       return HPDF_RaiseError (image->error, HPDF_INVALID_OPERATION, 0);

   name = HPDF_Image_GetColorSpace (smask);
   if (!name || HPDF_StrCmp (COL_GRAY, name) != 0)
       return HPDF_RaiseError (smask->error, HPDF_INVALID_COLOR_SPACE, 0);

   return HPDF_Dict_Add (image, "SMask", smask);
}

HPDF_STATUS
HPDF_Image_SetColorSpace  (HPDF_Image   image,
                          HPDF_Array   colorspace)
{
    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    return HPDF_Dict_Add (image, "ColorSpace", colorspace);
}


HPDF_STATUS
HPDF_Image_SetRenderingIntent  (HPDF_Image   image,
                          const char* intent)
{
    if (!HPDF_Image_Validate (image))
        return HPDF_INVALID_IMAGE;

    return HPDF_Dict_AddName (image, "Intent", intent);
}

