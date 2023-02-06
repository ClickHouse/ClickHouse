/*
 * << Haru Free PDF Library >> -- hpdf_pdfa.c
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
/* This is used to avoid warnings on 'ctime' when compiling in MSVC 9 */
#ifndef _CRT_SECURE_NO_WARNINGS
#define _CRT_SECURE_NO_WARNINGS
#endif

#include <time.h>
#include "hpdf_utils.h"
#include "hpdf.h"
#include <string.h>


#define HEADER                   "<?xpacket begin='' id='W5M0MpCehiHzreSzNTczkc9d'?><x:xmpmeta xmlns:x='adobe:ns:meta/' x:xmptk='XMP toolkit 2.9.1-13, framework 1.6'><rdf:RDF xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#' xmlns:iX='http://ns.adobe.com/iX/1.0/'>"
#define DC_HEADER                "<rdf:Description xmlns:dc='http://purl.org/dc/elements/1.1/' rdf:about=''>"
#define DC_TITLE_STARTTAG        "<dc:title><rdf:Alt><rdf:li xml:lang=\"x-default\">"
#define DC_TITLE_ENDTAG          "</rdf:li></rdf:Alt></dc:title>"
#define DC_CREATOR_STARTTAG      "<dc:creator><rdf:Seq><rdf:li>"
#define DC_CREATOR_ENDTAG        "</rdf:li></rdf:Seq></dc:creator>"
#define DC_DESCRIPTION_STARTTAG  "<dc:description><rdf:Alt><rdf:li xml:lang=\"x-default\">"
#define DC_DESCRIPTION_ENDTAG    "</rdf:li></rdf:Alt></dc:description>"
#define DC_FOOTER                "</rdf:Description>"
#define XMP_HEADER               "<rdf:Description xmlns:xmp='http://ns.adobe.com/xap/1.0/' rdf:about=''>"
#define XMP_CREATORTOOL_STARTTAG "<xmp:CreatorTool>"
#define XMP_CREATORTOOL_ENDTAG   "</xmp:CreatorTool>"
#define XMP_CREATE_DATE_STARTTAG "<xmp:CreateDate>"
#define XMP_CREATE_DATE_ENDTAG   "</xmp:CreateDate>"
#define XMP_MOD_DATE_STARTTAG    "<xmp:ModifyDate>"
#define XMP_MOD_DATE_ENDTAG      "</xmp:ModifyDate>"
#define XMP_FOOTER               "</rdf:Description>"
#define PDF_HEADER               "<rdf:Description xmlns:pdf='http://ns.adobe.com/pdf/1.3/' rdf:about=''>"
#define PDF_KEYWORDS_STARTTAG    "<pdf:Keywords>"
#define PDF_KEYWORDS_ENDTAG      "</pdf:Keywords>"
#define PDF_PRODUCER_STARTTAG    "<pdf:Producer>"
#define PDF_PRODUCER_ENDTAG      "</pdf:Producer>"
#define PDF_FOOTER               "</rdf:Description>"
#define PDFAID_PDFA1A            "<rdf:Description rdf:about='' xmlns:pdfaid='http://www.aiim.org/pdfa/ns/id/' pdfaid:part='1' pdfaid:conformance='A'/>"
#define PDFAID_PDFA1B            "<rdf:Description rdf:about='' xmlns:pdfaid='http://www.aiim.org/pdfa/ns/id/' pdfaid:part='1' pdfaid:conformance='B'/>"
#define FOOTER                   "</rdf:RDF></x:xmpmeta><?xpacket end='w'?>"


/*
 * Convert date in PDF specific format: D:YYYYMMDDHHmmSS
 * to XMP value in format YYYY-MM-DDTHH:mm:SS+offH:offMin
 */
HPDF_STATUS ConvertDateToXMDate(HPDF_Stream stream, const char *pDate)
{
    HPDF_STATUS ret;

    if(pDate==NULL) return HPDF_INVALID_PARAMETER;
    if(strlen(pDate)<16) return HPDF_INVALID_PARAMETER;
    if(pDate[0]!='D'||
        pDate[1]!=':') return HPDF_INVALID_PARAMETER;
    pDate+=2;
    /* Copy YYYY */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 4);
    if (ret != HPDF_OK)
        return ret;
    pDate+=4;
    /* Write -MM */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)"-", 1);
    if (ret != HPDF_OK)
        return ret;
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
    if (ret != HPDF_OK)
        return ret;
    pDate+=2;
    /* Write -DD */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)"-", 1);
    if (ret != HPDF_OK)
        return ret;
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
    if (ret != HPDF_OK)
        return ret;
    pDate+=2;
    /* Write THH */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)"T", 1);
    if (ret != HPDF_OK)
        return ret;
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
    if (ret != HPDF_OK)
        return ret;
    pDate+=2;
    /* Write :mm */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)":", 1);
    if (ret != HPDF_OK)
        return ret;
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
    if (ret != HPDF_OK)
        return ret;
    pDate+=2;
    /* Write :SS */
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)":", 1);
    if (ret != HPDF_OK)
        return ret;
    ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
    if (ret != HPDF_OK)
        return ret;
    pDate+=2;
    /* Write +... */
    if(pDate[0]==0) {
        ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)"Z", 1);
        return ret;
    }
    if(pDate[0]=='+'||pDate[0]=='-') {
        ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 3);
        if (ret != HPDF_OK)
            return ret;
        pDate+=4;
        ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)":", 1);
        if (ret != HPDF_OK)
            return ret;
        ret = HPDF_Stream_Write(stream, (const HPDF_BYTE*)pDate, 2);
        if (ret != HPDF_OK)
            return ret;
        return ret;
    }
    return HPDF_SetError (stream->error, HPDF_INVALID_PARAMETER, 0);
}

/* Write XMP Metadata for PDF/A */

HPDF_STATUS
HPDF_PDFA_SetPDFAConformance (HPDF_Doc pdf,HPDF_PDFAType pdfatype)
{
    HPDF_OutputIntent xmp;
    HPDF_STATUS ret;

    const char *dc_title       = NULL;
    const char *dc_creator     = NULL;
    const char *dc_description = NULL;

    const char *xmp_CreatorTool = NULL;
    const char *xmp_CreateDate  = NULL;
    const char *xmp_ModifyDate  = NULL;

    const char *pdf_Keywords    = NULL;
    const char *pdf_Producer    = NULL;

    const char *info = NULL;

    if (!HPDF_HasDoc(pdf)) {
      return HPDF_INVALID_DOCUMENT;
    }

    dc_title       = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_TITLE);
    dc_creator     = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_AUTHOR);
    dc_description = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_SUBJECT);

    xmp_CreateDate  = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_CREATION_DATE);
    xmp_ModifyDate  = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_MOD_DATE);
    xmp_CreatorTool = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_CREATOR);

    pdf_Keywords = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_KEYWORDS);
    pdf_Producer = (const char *)HPDF_GetInfoAttr(pdf, HPDF_INFO_PRODUCER);

    if((dc_title != NULL) || (dc_creator != NULL) || (dc_description != NULL)
       || (xmp_CreateDate != NULL) || (xmp_ModifyDate != NULL) || (xmp_CreatorTool != NULL)
       || (pdf_Keywords != NULL)) {

        xmp = HPDF_DictStream_New(pdf->mmgr,pdf->xref);
        if (!xmp) {
          return HPDF_INVALID_STREAM;
        }

        /* Update the PDF number version */
        pdf->pdf_version = HPDF_VER_14;

        HPDF_Dict_AddName(xmp,"Type","Metadata");
        HPDF_Dict_AddName(xmp,"SubType","XML");

        ret = HPDF_OK;
        ret += HPDF_Stream_WriteStr(xmp->stream, HEADER);

        /* Add the dc block */
        if((dc_title != NULL) || (dc_creator != NULL) || (dc_description != NULL)) {
            ret += HPDF_Stream_WriteStr(xmp->stream, DC_HEADER);

            if(dc_title != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_TITLE_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, dc_title);
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_TITLE_ENDTAG);
            }

            if(dc_creator != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_CREATOR_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, dc_creator);
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_CREATOR_ENDTAG);
            }

            if(dc_description != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_DESCRIPTION_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, dc_description);
                ret += HPDF_Stream_WriteStr(xmp->stream, DC_DESCRIPTION_ENDTAG);
            }

            ret += HPDF_Stream_WriteStr(xmp->stream, DC_FOOTER);
        }

        /* Add the xmp block */
        if((xmp_CreateDate != NULL) || (xmp_ModifyDate != NULL) || (xmp_CreatorTool != NULL)) {
            ret += HPDF_Stream_WriteStr(xmp->stream, XMP_HEADER);

            /* Add CreateDate, ModifyDate, and CreatorTool */
            if(xmp_CreatorTool != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_CREATORTOOL_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, xmp_CreatorTool);
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_CREATORTOOL_ENDTAG);
            }

            if(xmp_CreateDate != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_CREATE_DATE_STARTTAG);
                /* Convert date to XMP compatible format */
                ret += ConvertDateToXMDate(xmp->stream, xmp_CreateDate);
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_CREATE_DATE_ENDTAG);
            }

            if(xmp_ModifyDate != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_MOD_DATE_STARTTAG);
                ret += ConvertDateToXMDate(xmp->stream, xmp_ModifyDate);
                ret += HPDF_Stream_WriteStr(xmp->stream, XMP_MOD_DATE_ENDTAG);
            }

            ret += HPDF_Stream_WriteStr(xmp->stream, XMP_FOOTER);
        }

        /* Add the pdf block */
        if((pdf_Keywords != NULL) || (pdf_Producer != NULL)) {
            ret += HPDF_Stream_WriteStr(xmp->stream, PDF_HEADER);

            if(pdf_Keywords != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, PDF_KEYWORDS_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, pdf_Keywords);
                ret += HPDF_Stream_WriteStr(xmp->stream, PDF_KEYWORDS_ENDTAG);
            }

            if(pdf_Producer != NULL) {
                ret += HPDF_Stream_WriteStr(xmp->stream, PDF_PRODUCER_STARTTAG);
                ret += HPDF_Stream_WriteStr(xmp->stream, pdf_Producer);
                ret += HPDF_Stream_WriteStr(xmp->stream, PDF_PRODUCER_ENDTAG);
            }

            ret += HPDF_Stream_WriteStr(xmp->stream, PDF_FOOTER);
        }

        /* Add the pdfaid block */
        switch(pdfatype) {
          case HPDF_PDFA_1A:
            ret += HPDF_Stream_WriteStr(xmp->stream, PDFAID_PDFA1A);
            break;
          case HPDF_PDFA_1B:
            ret += HPDF_Stream_WriteStr(xmp->stream, PDFAID_PDFA1B);
            break;
        }

        ret += HPDF_Stream_WriteStr(xmp->stream, FOOTER);

        if (ret != HPDF_OK) {
          return HPDF_INVALID_STREAM;
        }

        if ((ret = HPDF_Dict_Add(pdf->catalog, "Metadata", xmp)) != HPDF_OK) {
          return ret;
        }

        return HPDF_PDFA_GenerateID(pdf);
    }

    return HPDF_OK;
}

/* Generate an ID for the trailer dict, PDF/A needs this. 
   TODO: Better algorithm for generate unique ID.
*/
HPDF_STATUS
HPDF_PDFA_GenerateID(HPDF_Doc pdf)
{
    HPDF_Array id;
    HPDF_BYTE *currentTime;
    HPDF_BYTE idkey[HPDF_MD5_KEY_LEN];
    HPDF_MD5_CTX md5_ctx;
    time_t ltime; 

    ltime = time(NULL); 
    currentTime = (HPDF_BYTE *)ctime(&ltime);
        
    id = HPDF_Dict_GetItem(pdf->trailer, "ID", HPDF_OCLASS_ARRAY);
    if (!id) {
       id = HPDF_Array_New(pdf->mmgr);

       if (!id || HPDF_Dict_Add(pdf->trailer, "ID", id) != HPDF_OK)
         return pdf->error.error_no;
       
       HPDF_MD5Init(&md5_ctx);
       HPDF_MD5Update(&md5_ctx, (HPDF_BYTE *) "libHaru", sizeof("libHaru") - 1);
       HPDF_MD5Update(&md5_ctx, currentTime, HPDF_StrLen((const char *)currentTime, -1));
       HPDF_MD5Final(idkey, &md5_ctx);
       
       if (HPDF_Array_Add (id, HPDF_Binary_New (pdf->mmgr, idkey, HPDF_MD5_KEY_LEN)) != HPDF_OK)
         return pdf->error.error_no;

       if (HPDF_Array_Add (id, HPDF_Binary_New (pdf->mmgr,idkey,HPDF_MD5_KEY_LEN)) != HPDF_OK)
         return pdf->error.error_no;
    
       return HPDF_OK;
    }
    
    return HPDF_OK;
}

/* Function to add one outputintents to the PDF
 * iccname - name of default ICC profile
 * iccdict - dictionary containing number of components
 *           and stream with ICC profile
 *
 * How to use:
 * 1. Create dictionary with ICC profile
 *    HPDF_Dict icc = HPDF_DictStream_New (pDoc->mmgr, pDoc->xref);
 *    if(icc==NULL) return false;
 *    HPDF_Dict_AddNumber (icc, "N", 3);
 *    HPDF_STATUS ret = HPDF_Stream_Write (icc->stream, (const HPDF_BYTE *)pICCData, dwICCSize);
 *    if(ret!=HPDF_OK) {
 *      HPDF_Dict_Free(icc);
 *      return false;
 *    }
 *
 * 2. Call this function
 */

HPDF_STATUS
HPDF_PDFA_AppendOutputIntents(HPDF_Doc pdf, const char *iccname, HPDF_Dict iccdict)
{
    HPDF_Array intents;
    HPDF_Dict intent;
    HPDF_STATUS ret;
    if (!HPDF_HasDoc (pdf))
        return HPDF_INVALID_DOCUMENT;

    /* prepare intent */
    intent = HPDF_Dict_New (pdf->mmgr);
    ret = HPDF_Xref_Add (pdf->xref, intent);
    if ( ret != HPDF_OK) {
        HPDF_Dict_Free(intent);
        return ret;
    }
    ret += HPDF_Dict_AddName (intent, "Type", "OutputIntent");
    ret += HPDF_Dict_AddName (intent, "S", "GTS_PDFA1");
    ret += HPDF_Dict_Add (intent, "OutputConditionIdentifier", HPDF_String_New (pdf->mmgr, iccname, NULL));
    ret += HPDF_Dict_Add (intent, "OutputCondition", HPDF_String_New (pdf->mmgr, iccname,NULL));
    ret += HPDF_Dict_Add (intent, "Info", HPDF_String_New (pdf->mmgr, iccname, NULL));
    ret += HPDF_Dict_Add (intent, "DestOutputProfile ", iccdict);
    if ( ret != HPDF_OK) {
        HPDF_Dict_Free(intent);
        return ret;
    }

    /* Copied from HPDF_AddIntent - not public function */
    intents = HPDF_Dict_GetItem (pdf->catalog, "OutputIntents", HPDF_OCLASS_ARRAY);
    if (intents == NULL) {
        intents = HPDF_Array_New (pdf->mmgr);
        if (intents) {
            HPDF_STATUS ret = HPDF_Dict_Add (pdf->catalog, "OutputIntents", intents);
            if (ret != HPDF_OK) {
                HPDF_CheckError (&pdf->error);
                return HPDF_Error_GetDetailCode (&pdf->error);
            }
        }
    }

    HPDF_Array_Add(intents,intent);
    return HPDF_Error_GetDetailCode (&pdf->error);
}
