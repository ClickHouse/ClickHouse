/*
 * << Haru Free PDF Library >> -- hpdf_consts.h
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


#ifndef _HPDF_CONSTS_H
#define _HPDF_CONSTS_H

/*----------------------------------------------------------------------------*/

#define  HPDF_TRUE                  1
#define  HPDF_FALSE                 0

#define  HPDF_OK                    0
#define  HPDF_NOERROR               0

/*----- default values -------------------------------------------------------*/

/* buffer size which is required when we convert to character string. */
#define HPDF_TMP_BUF_SIZ            512
#define HPDF_SHORT_BUF_SIZ          32
#define HPDF_REAL_LEN               11
#define HPDF_INT_LEN                11
#define HPDF_TEXT_DEFAULT_LEN       256
#define HPDF_UNICODE_HEADER_LEN     2
#define HPDF_DATE_TIME_STR_LEN      23

/* length of each item defined in PDF */
#define HPDF_BYTE_OFFSET_LEN        10
#define HPDF_OBJ_ID_LEN             7
#define HPDF_GEN_NO_LEN             5

/* default value of Graphic State */
#define HPDF_DEF_FONT               "Helvetica"
#define HPDF_DEF_PAGE_LAYOUT        HPDF_PAGE_LAYOUT_SINGLE
#define HPDF_DEF_PAGE_MODE          HPDF_PAGE_MODE_USE_NONE
#define HPDF_DEF_WORDSPACE          0
#define HPDF_DEF_CHARSPACE          0
#define HPDF_DEF_FONTSIZE           10
#define HPDF_DEF_HSCALING           100
#define HPDF_DEF_LEADING            0
#define HPDF_DEF_RENDERING_MODE     HPDF_FILL
#define HPDF_DEF_RISE               0
#define HPDF_DEF_RAISE              HPDF_DEF_RISE
#define HPDF_DEF_LINEWIDTH          1
#define HPDF_DEF_LINECAP            HPDF_BUTT_END
#define HPDF_DEF_LINEJOIN           HPDF_MITER_JOIN
#define HPDF_DEF_MITERLIMIT         10
#define HPDF_DEF_FLATNESS           1
#define HPDF_DEF_PAGE_NUM           1

#define HPDF_BS_DEF_WIDTH           1

/* defalt page-size */
#define HPDF_DEF_PAGE_WIDTH         595.276F
#define HPDF_DEF_PAGE_HEIGHT        841.89F

/*---------------------------------------------------------------------------*/
/*----- compression mode ----------------------------------------------------*/

#define  HPDF_COMP_NONE            0x00
#define  HPDF_COMP_TEXT            0x01
#define  HPDF_COMP_IMAGE           0x02
#define  HPDF_COMP_METADATA        0x04
#define  HPDF_COMP_ALL             0x0F
/* #define  HPDF_COMP_BEST_COMPRESS   0x10
 * #define  HPDF_COMP_BEST_SPEED      0x20
 */
#define  HPDF_COMP_MASK            0xFF


/*----------------------------------------------------------------------------*/
/*----- permission flags (only Revision 2 is supported)-----------------------*/

#define HPDF_ENABLE_READ         0
#define HPDF_ENABLE_PRINT        4
#define HPDF_ENABLE_EDIT_ALL     8
#define HPDF_ENABLE_COPY         16
#define HPDF_ENABLE_EDIT         32


/*----------------------------------------------------------------------------*/
/*------ viewer preferences definitions --------------------------------------*/

#define HPDF_HIDE_TOOLBAR    1
#define HPDF_HIDE_MENUBAR    2
#define HPDF_HIDE_WINDOW_UI  4
#define HPDF_FIT_WINDOW      8
#define HPDF_CENTER_WINDOW   16
#define HPDF_PRINT_SCALING_NONE   32


/*---------------------------------------------------------------------------*/
/*------ limitation of object implementation (PDF1.4) -----------------------*/

#define HPDF_LIMIT_MAX_INT             2147483647
#define HPDF_LIMIT_MIN_INT             -2147483647

#define HPDF_LIMIT_MAX_REAL            32767
#define HPDF_LIMIT_MIN_REAL            -32767

#define HPDF_LIMIT_MAX_STRING_LEN      65535
#define HPDF_LIMIT_MAX_NAME_LEN        127

#define HPDF_LIMIT_MAX_ARRAY           32767
#define HPDF_LIMIT_MAX_DICT_ELEMENT    4095
#define HPDF_LIMIT_MAX_XREF_ELEMENT    8388607
#define HPDF_LIMIT_MAX_GSTATE          28
#define HPDF_LIMIT_MAX_DEVICE_N        8
#define HPDF_LIMIT_MAX_DEVICE_N_V15    32
#define HPDF_LIMIT_MAX_CID             65535
#define HPDF_MAX_GENERATION_NUM        65535

#define HPDF_MIN_PAGE_HEIGHT           3
#define HPDF_MIN_PAGE_WIDTH            3
#define HPDF_MAX_PAGE_HEIGHT           14400
#define HPDF_MAX_PAGE_WIDTH            14400
#define HPDF_MIN_MAGNIFICATION_FACTOR  8
#define HPDF_MAX_MAGNIFICATION_FACTOR  3200

/*---------------------------------------------------------------------------*/
/*------ limitation of various properties -----------------------------------*/

#define HPDF_MIN_PAGE_SIZE          3
#define HPDF_MAX_PAGE_SIZE          14400
#define HPDF_MIN_HORIZONTALSCALING  10
#define HPDF_MAX_HORIZONTALSCALING  300
#define HPDF_MIN_WORDSPACE          -30
#define HPDF_MAX_WORDSPACE          300
#define HPDF_MIN_CHARSPACE          -30
#define HPDF_MAX_CHARSPACE          300
#define HPDF_MAX_FONTSIZE           600
#define HPDF_MAX_ZOOMSIZE           10
#define HPDF_MAX_LEADING            300
#define HPDF_MAX_LINEWIDTH          100
#define HPDF_MAX_DASH_PATTERN       100

#define HPDF_MAX_JWW_NUM            128

/*----------------------------------------------------------------------------*/
/*----- country code definition ----------------------------------------------*/

#define HPDF_COUNTRY_AF  "AF"    /* AFGHANISTAN */
#define HPDF_COUNTRY_AL  "AL"    /* ALBANIA */
#define HPDF_COUNTRY_DZ  "DZ"    /* ALGERIA */
#define HPDF_COUNTRY_AS  "AS"    /* AMERICAN SAMOA */
#define HPDF_COUNTRY_AD  "AD"    /* ANDORRA */
#define HPDF_COUNTRY_AO  "AO"    /* ANGOLA */
#define HPDF_COUNTRY_AI  "AI"    /* ANGUILLA */
#define HPDF_COUNTRY_AQ  "AQ"    /* ANTARCTICA */
#define HPDF_COUNTRY_AG  "AG"    /* ANTIGUA AND BARBUDA */
#define HPDF_COUNTRY_AR  "AR"    /* ARGENTINA */
#define HPDF_COUNTRY_AM  "AM"    /* ARMENIA */
#define HPDF_COUNTRY_AW  "AW"    /* ARUBA */
#define HPDF_COUNTRY_AU  "AU"    /* AUSTRALIA */
#define HPDF_COUNTRY_AT  "AT"    /* AUSTRIA */
#define HPDF_COUNTRY_AZ  "AZ"    /* AZERBAIJAN */
#define HPDF_COUNTRY_BS  "BS"    /* BAHAMAS */
#define HPDF_COUNTRY_BH  "BH"    /* BAHRAIN */
#define HPDF_COUNTRY_BD  "BD"    /* BANGLADESH */
#define HPDF_COUNTRY_BB  "BB"    /* BARBADOS */
#define HPDF_COUNTRY_BY  "BY"    /* BELARUS */
#define HPDF_COUNTRY_BE  "BE"    /* BELGIUM */
#define HPDF_COUNTRY_BZ  "BZ"    /* BELIZE */
#define HPDF_COUNTRY_BJ  "BJ"    /* BENIN */
#define HPDF_COUNTRY_BM  "BM"    /* BERMUDA */
#define HPDF_COUNTRY_BT  "BT"    /* BHUTAN */
#define HPDF_COUNTRY_BO  "BO"    /* BOLIVIA */
#define HPDF_COUNTRY_BA  "BA"    /* BOSNIA AND HERZEGOWINA */
#define HPDF_COUNTRY_BW  "BW"    /* BOTSWANA */
#define HPDF_COUNTRY_BV  "BV"    /* BOUVET ISLAND */
#define HPDF_COUNTRY_BR  "BR"    /* BRAZIL */
#define HPDF_COUNTRY_IO  "IO"    /* BRITISH INDIAN OCEAN TERRITORY */
#define HPDF_COUNTRY_BN  "BN"    /* BRUNEI DARUSSALAM */
#define HPDF_COUNTRY_BG  "BG"    /* BULGARIA */
#define HPDF_COUNTRY_BF  "BF"    /* BURKINA FASO */
#define HPDF_COUNTRY_BI  "BI"    /* BURUNDI */
#define HPDF_COUNTRY_KH  "KH"    /* CAMBODIA */
#define HPDF_COUNTRY_CM  "CM"    /* CAMEROON */
#define HPDF_COUNTRY_CA  "CA"    /* CANADA */
#define HPDF_COUNTRY_CV  "CV"    /* CAPE VERDE */
#define HPDF_COUNTRY_KY  "KY"    /* CAYMAN ISLANDS */
#define HPDF_COUNTRY_CF  "CF"    /* CENTRAL AFRICAN REPUBLIC */
#define HPDF_COUNTRY_TD  "TD"    /* CHAD */
#define HPDF_COUNTRY_CL  "CL"    /* CHILE */
#define HPDF_COUNTRY_CN  "CN"    /* CHINA */
#define HPDF_COUNTRY_CX  "CX"    /* CHRISTMAS ISLAND */
#define HPDF_COUNTRY_CC  "CC"    /* COCOS (KEELING) ISLANDS */
#define HPDF_COUNTRY_CO  "CO"    /* COLOMBIA */
#define HPDF_COUNTRY_KM  "KM"    /* COMOROS */
#define HPDF_COUNTRY_CG  "CG"    /* CONGO */
#define HPDF_COUNTRY_CK  "CK"    /* COOK ISLANDS */
#define HPDF_COUNTRY_CR  "CR"    /* COSTA RICA */
#define HPDF_COUNTRY_CI  "CI"    /* COTE D'IVOIRE */
#define HPDF_COUNTRY_HR  "HR"    /* CROATIA (local name: Hrvatska) */
#define HPDF_COUNTRY_CU  "CU"    /* CUBA */
#define HPDF_COUNTRY_CY  "CY"    /* CYPRUS */
#define HPDF_COUNTRY_CZ  "CZ"    /* CZECH REPUBLIC */
#define HPDF_COUNTRY_DK  "DK"    /* DENMARK */
#define HPDF_COUNTRY_DJ  "DJ"    /* DJIBOUTI */
#define HPDF_COUNTRY_DM  "DM"    /* DOMINICA */
#define HPDF_COUNTRY_DO  "DO"    /* DOMINICAN REPUBLIC */
#define HPDF_COUNTRY_TP  "TP"    /* EAST TIMOR */
#define HPDF_COUNTRY_EC  "EC"    /* ECUADOR */
#define HPDF_COUNTRY_EG  "EG"    /* EGYPT */
#define HPDF_COUNTRY_SV  "SV"    /* EL SALVADOR */
#define HPDF_COUNTRY_GQ  "GQ"    /* EQUATORIAL GUINEA */
#define HPDF_COUNTRY_ER  "ER"    /* ERITREA */
#define HPDF_COUNTRY_EE  "EE"    /* ESTONIA */
#define HPDF_COUNTRY_ET  "ET"    /* ETHIOPIA */
#define HPDF_COUNTRY_FK  "FK"   /* FALKLAND ISLANDS (MALVINAS) */
#define HPDF_COUNTRY_FO  "FO"    /* FAROE ISLANDS */
#define HPDF_COUNTRY_FJ  "FJ"    /* FIJI */
#define HPDF_COUNTRY_FI  "FI"    /* FINLAND */
#define HPDF_COUNTRY_FR  "FR"    /* FRANCE */
#define HPDF_COUNTRY_FX  "FX"    /* FRANCE, METROPOLITAN */
#define HPDF_COUNTRY_GF  "GF"    /* FRENCH GUIANA */
#define HPDF_COUNTRY_PF  "PF"    /* FRENCH POLYNESIA */
#define HPDF_COUNTRY_TF  "TF"    /* FRENCH SOUTHERN TERRITORIES */
#define HPDF_COUNTRY_GA  "GA"    /* GABON */
#define HPDF_COUNTRY_GM  "GM"    /* GAMBIA */
#define HPDF_COUNTRY_GE  "GE"    /* GEORGIA */
#define HPDF_COUNTRY_DE  "DE"    /* GERMANY */
#define HPDF_COUNTRY_GH  "GH"    /* GHANA */
#define HPDF_COUNTRY_GI  "GI"    /* GIBRALTAR */
#define HPDF_COUNTRY_GR  "GR"    /* GREECE */
#define HPDF_COUNTRY_GL  "GL"    /* GREENLAND */
#define HPDF_COUNTRY_GD  "GD"    /* GRENADA */
#define HPDF_COUNTRY_GP  "GP"    /* GUADELOUPE */
#define HPDF_COUNTRY_GU  "GU"    /* GUAM */
#define HPDF_COUNTRY_GT  "GT"    /* GUATEMALA */
#define HPDF_COUNTRY_GN  "GN"    /* GUINEA */
#define HPDF_COUNTRY_GW  "GW"    /* GUINEA-BISSAU */
#define HPDF_COUNTRY_GY  "GY"    /* GUYANA */
#define HPDF_COUNTRY_HT  "HT"    /* HAITI */
#define HPDF_COUNTRY_HM  "HM"    /* HEARD AND MC DONALD ISLANDS */
#define HPDF_COUNTRY_HN  "HN"    /* HONDURAS */
#define HPDF_COUNTRY_HK  "HK"    /* HONG KONG */
#define HPDF_COUNTRY_HU  "HU"    /* HUNGARY */
#define HPDF_COUNTRY_IS  "IS"    /* ICELAND */
#define HPDF_COUNTRY_IN  "IN"    /* INDIA */
#define HPDF_COUNTRY_ID  "ID"    /* INDONESIA */
#define HPDF_COUNTRY_IR  "IR"    /* IRAN (ISLAMIC REPUBLIC OF) */
#define HPDF_COUNTRY_IQ  "IQ"    /* IRAQ */
#define HPDF_COUNTRY_IE  "IE"    /* IRELAND */
#define HPDF_COUNTRY_IL  "IL"    /* ISRAEL */
#define HPDF_COUNTRY_IT  "IT"    /* ITALY */
#define HPDF_COUNTRY_JM  "JM"    /* JAMAICA */
#define HPDF_COUNTRY_JP  "JP"    /* JAPAN */
#define HPDF_COUNTRY_JO  "JO"    /* JORDAN */
#define HPDF_COUNTRY_KZ  "KZ"    /* KAZAKHSTAN */
#define HPDF_COUNTRY_KE  "KE"    /* KENYA */
#define HPDF_COUNTRY_KI  "KI"    /* KIRIBATI */
#define HPDF_COUNTRY_KP  "KP"    /* KOREA, DEMOCRATIC PEOPLE'S REPUBLIC OF */
#define HPDF_COUNTRY_KR  "KR"    /* KOREA, REPUBLIC OF */
#define HPDF_COUNTRY_KW  "KW"    /* KUWAIT */
#define HPDF_COUNTRY_KG  "KG"    /* KYRGYZSTAN */
#define HPDF_COUNTRY_LA  "LA"    /* LAO PEOPLE'S DEMOCRATIC REPUBLIC */
#define HPDF_COUNTRY_LV  "LV"    /* LATVIA */
#define HPDF_COUNTRY_LB  "LB"    /* LEBANON */
#define HPDF_COUNTRY_LS  "LS"    /* LESOTHO */
#define HPDF_COUNTRY_LR  "LR"    /* LIBERIA */
#define HPDF_COUNTRY_LY  "LY"    /* LIBYAN ARAB JAMAHIRIYA */
#define HPDF_COUNTRY_LI  "LI"    /* LIECHTENSTEIN */
#define HPDF_COUNTRY_LT  "LT"    /* LITHUANIA */
#define HPDF_COUNTRY_LU  "LU"    /* LUXEMBOURG */
#define HPDF_COUNTRY_MO  "MO"    /* MACAU */
#define HPDF_COUNTRY_MK  "MK"   /* MACEDONIA, THE FORMER YUGOSLAV REPUBLIC OF */
#define HPDF_COUNTRY_MG  "MG"    /* MADAGASCAR */
#define HPDF_COUNTRY_MW  "MW"    /* MALAWI */
#define HPDF_COUNTRY_MY  "MY"    /* MALAYSIA */
#define HPDF_COUNTRY_MV  "MV"    /* MALDIVES */
#define HPDF_COUNTRY_ML  "ML"    /* MALI */
#define HPDF_COUNTRY_MT  "MT"    /* MALTA */
#define HPDF_COUNTRY_MH  "MH"    /* MARSHALL ISLANDS */
#define HPDF_COUNTRY_MQ  "MQ"    /* MARTINIQUE */
#define HPDF_COUNTRY_MR  "MR"    /* MAURITANIA */
#define HPDF_COUNTRY_MU  "MU"    /* MAURITIUS */
#define HPDF_COUNTRY_YT  "YT"    /* MAYOTTE */
#define HPDF_COUNTRY_MX  "MX"    /* MEXICO */
#define HPDF_COUNTRY_FM  "FM"    /* MICRONESIA, FEDERATED STATES OF */
#define HPDF_COUNTRY_MD  "MD"    /* MOLDOVA, REPUBLIC OF */
#define HPDF_COUNTRY_MC  "MC"    /* MONACO */
#define HPDF_COUNTRY_MN  "MN"    /* MONGOLIA */
#define HPDF_COUNTRY_MS  "MS"    /* MONTSERRAT */
#define HPDF_COUNTRY_MA  "MA"    /* MOROCCO */
#define HPDF_COUNTRY_MZ  "MZ"    /* MOZAMBIQUE */
#define HPDF_COUNTRY_MM  "MM"    /* MYANMAR */
#define HPDF_COUNTRY_NA  "NA"    /* NAMIBIA */
#define HPDF_COUNTRY_NR  "NR"    /* NAURU */
#define HPDF_COUNTRY_NP  "NP"    /* NEPAL */
#define HPDF_COUNTRY_NL  "NL"    /* NETHERLANDS */
#define HPDF_COUNTRY_AN  "AN"    /* NETHERLANDS ANTILLES */
#define HPDF_COUNTRY_NC  "NC"    /* NEW CALEDONIA */
#define HPDF_COUNTRY_NZ  "NZ"    /* NEW ZEALAND */
#define HPDF_COUNTRY_NI  "NI"    /* NICARAGUA */
#define HPDF_COUNTRY_NE  "NE"    /* NIGER */
#define HPDF_COUNTRY_NG  "NG"    /* NIGERIA */
#define HPDF_COUNTRY_NU  "NU"    /* NIUE */
#define HPDF_COUNTRY_NF  "NF"    /* NORFOLK ISLAND */
#define HPDF_COUNTRY_MP  "MP"    /* NORTHERN MARIANA ISLANDS */
#define HPDF_COUNTRY_NO  "NO"    /* NORWAY */
#define HPDF_COUNTRY_OM  "OM"    /* OMAN */
#define HPDF_COUNTRY_PK  "PK"    /* PAKISTAN */
#define HPDF_COUNTRY_PW  "PW"    /* PALAU */
#define HPDF_COUNTRY_PA  "PA"    /* PANAMA */
#define HPDF_COUNTRY_PG  "PG"    /* PAPUA NEW GUINEA */
#define HPDF_COUNTRY_PY  "PY"    /* PARAGUAY */
#define HPDF_COUNTRY_PE  "PE"    /* PERU */
#define HPDF_COUNTRY_PH  "PH"    /* PHILIPPINES */
#define HPDF_COUNTRY_PN  "PN"    /* PITCAIRN */
#define HPDF_COUNTRY_PL  "PL"    /* POLAND */
#define HPDF_COUNTRY_PT  "PT"    /* PORTUGAL */
#define HPDF_COUNTRY_PR  "PR"    /* PUERTO RICO */
#define HPDF_COUNTRY_QA  "QA"    /* QATAR */
#define HPDF_COUNTRY_RE  "RE"    /* REUNION */
#define HPDF_COUNTRY_RO  "RO"    /* ROMANIA */
#define HPDF_COUNTRY_RU  "RU"    /* RUSSIAN FEDERATION */
#define HPDF_COUNTRY_RW  "RW"    /* RWANDA */
#define HPDF_COUNTRY_KN  "KN"    /* SAINT KITTS AND NEVIS */
#define HPDF_COUNTRY_LC  "LC"    /* SAINT LUCIA */
#define HPDF_COUNTRY_VC  "VC"    /* SAINT VINCENT AND THE GRENADINES */
#define HPDF_COUNTRY_WS  "WS"    /* SAMOA */
#define HPDF_COUNTRY_SM  "SM"    /* SAN MARINO */
#define HPDF_COUNTRY_ST  "ST"    /* SAO TOME AND PRINCIPE */
#define HPDF_COUNTRY_SA  "SA"    /* SAUDI ARABIA */
#define HPDF_COUNTRY_SN  "SN"    /* SENEGAL */
#define HPDF_COUNTRY_SC  "SC"    /* SEYCHELLES */
#define HPDF_COUNTRY_SL  "SL"    /* SIERRA LEONE */
#define HPDF_COUNTRY_SG  "SG"    /* SINGAPORE */
#define HPDF_COUNTRY_SK  "SK"    /* SLOVAKIA (Slovak Republic) */
#define HPDF_COUNTRY_SI  "SI"    /* SLOVENIA */
#define HPDF_COUNTRY_SB  "SB"    /* SOLOMON ISLANDS */
#define HPDF_COUNTRY_SO  "SO"    /* SOMALIA */
#define HPDF_COUNTRY_ZA  "ZA"    /* SOUTH AFRICA */
#define HPDF_COUNTRY_ES  "ES"    /* SPAIN */
#define HPDF_COUNTRY_LK  "LK"    /* SRI LANKA */
#define HPDF_COUNTRY_SH  "SH"    /* ST. HELENA */
#define HPDF_COUNTRY_PM  "PM"    /* ST. PIERRE AND MIQUELON */
#define HPDF_COUNTRY_SD  "SD"    /* SUDAN */
#define HPDF_COUNTRY_SR  "SR"    /* SURINAME */
#define HPDF_COUNTRY_SJ  "SJ"    /* SVALBARD AND JAN MAYEN ISLANDS */
#define HPDF_COUNTRY_SZ  "SZ"    /* SWAZILAND */
#define HPDF_COUNTRY_SE  "SE"    /* SWEDEN */
#define HPDF_COUNTRY_CH  "CH"    /* SWITZERLAND */
#define HPDF_COUNTRY_SY  "SY"    /* SYRIAN ARAB REPUBLIC */
#define HPDF_COUNTRY_TW  "TW"    /* TAIWAN, PROVINCE OF CHINA */
#define HPDF_COUNTRY_TJ  "TJ"    /* TAJIKISTAN */
#define HPDF_COUNTRY_TZ  "TZ"    /* TANZANIA, UNITED REPUBLIC OF */
#define HPDF_COUNTRY_TH  "TH"    /* THAILAND */
#define HPDF_COUNTRY_TG  "TG"    /* TOGO */
#define HPDF_COUNTRY_TK  "TK"    /* TOKELAU */
#define HPDF_COUNTRY_TO  "TO"    /* TONGA */
#define HPDF_COUNTRY_TT  "TT"    /* TRINIDAD AND TOBAGO */
#define HPDF_COUNTRY_TN  "TN"    /* TUNISIA */
#define HPDF_COUNTRY_TR  "TR"    /* TURKEY */
#define HPDF_COUNTRY_TM  "TM"    /* TURKMENISTAN */
#define HPDF_COUNTRY_TC  "TC"    /* TURKS AND CAICOS ISLANDS */
#define HPDF_COUNTRY_TV  "TV"    /* TUVALU */
#define HPDF_COUNTRY_UG  "UG"    /* UGANDA */
#define HPDF_COUNTRY_UA  "UA"    /* UKRAINE */
#define HPDF_COUNTRY_AE  "AE"    /* UNITED ARAB EMIRATES */
#define HPDF_COUNTRY_GB  "GB"    /* UNITED KINGDOM */
#define HPDF_COUNTRY_US  "US"    /* UNITED STATES */
#define HPDF_COUNTRY_UM  "UM"    /* UNITED STATES MINOR OUTLYING ISLANDS */
#define HPDF_COUNTRY_UY  "UY"    /* URUGUAY */
#define HPDF_COUNTRY_UZ  "UZ"    /* UZBEKISTAN */
#define HPDF_COUNTRY_VU  "VU"    /* VANUATU */
#define HPDF_COUNTRY_VA  "VA"    /* VATICAN CITY STATE (HOLY SEE) */
#define HPDF_COUNTRY_VE  "VE"    /* VENEZUELA */
#define HPDF_COUNTRY_VN  "VN"    /* VIET NAM */
#define HPDF_COUNTRY_VG  "VG"    /* VIRGIN ISLANDS (BRITISH) */
#define HPDF_COUNTRY_VI  "VI"    /* VIRGIN ISLANDS (U.S.) */
#define HPDF_COUNTRY_WF  "WF"    /* WALLIS AND FUTUNA ISLANDS */
#define HPDF_COUNTRY_EH  "EH"    /* WESTERN SAHARA */
#define HPDF_COUNTRY_YE  "YE"    /* YEMEN */
#define HPDF_COUNTRY_YU  "YU"    /* YUGOSLAVIA */
#define HPDF_COUNTRY_ZR  "ZR"    /* ZAIRE */
#define HPDF_COUNTRY_ZM  "ZM"    /* ZAMBIA */
#define HPDF_COUNTRY_ZW  "ZW"    /* ZIMBABWE */

/*----------------------------------------------------------------------------*/
/*----- lang code definition -------------------------------------------------*/

#define HPDF_LANG_AA    "aa"     /* Afar */
#define HPDF_LANG_AB    "ab"     /* Abkhazian */
#define HPDF_LANG_AF    "af"     /* Afrikaans */
#define HPDF_LANG_AM    "am"     /* Amharic */
#define HPDF_LANG_AR    "ar"     /* Arabic */
#define HPDF_LANG_AS    "as"     /* Assamese */
#define HPDF_LANG_AY    "ay"     /* Aymara */
#define HPDF_LANG_AZ    "az"     /* Azerbaijani */
#define HPDF_LANG_BA    "ba"     /* Bashkir */
#define HPDF_LANG_BE    "be"     /* Byelorussian */
#define HPDF_LANG_BG    "bg"     /* Bulgarian */
#define HPDF_LANG_BH    "bh"     /* Bihari */
#define HPDF_LANG_BI    "bi"     /* Bislama */
#define HPDF_LANG_BN    "bn"     /* Bengali Bangla */
#define HPDF_LANG_BO    "bo"     /* Tibetan */
#define HPDF_LANG_BR    "br"     /* Breton */
#define HPDF_LANG_CA    "ca"     /* Catalan */
#define HPDF_LANG_CO    "co"     /* Corsican */
#define HPDF_LANG_CS    "cs"     /* Czech */
#define HPDF_LANG_CY    "cy"     /* Welsh */
#define HPDF_LANG_DA    "da"     /* Danish */
#define HPDF_LANG_DE    "de"     /* German */
#define HPDF_LANG_DZ    "dz"     /* Bhutani */
#define HPDF_LANG_EL    "el"     /* Greek */
#define HPDF_LANG_EN    "en"     /* English */
#define HPDF_LANG_EO    "eo"     /* Esperanto */
#define HPDF_LANG_ES    "es"     /* Spanish */
#define HPDF_LANG_ET    "et"     /* Estonian */
#define HPDF_LANG_EU    "eu"     /* Basque */
#define HPDF_LANG_FA    "fa"     /* Persian */
#define HPDF_LANG_FI    "fi"     /* Finnish */
#define HPDF_LANG_FJ    "fj"     /* Fiji */
#define HPDF_LANG_FO    "fo"     /* Faeroese */
#define HPDF_LANG_FR    "fr"     /* French */
#define HPDF_LANG_FY    "fy"     /* Frisian */
#define HPDF_LANG_GA    "ga"     /* Irish */
#define HPDF_LANG_GD    "gd"     /* Scots Gaelic */
#define HPDF_LANG_GL    "gl"     /* Galician */
#define HPDF_LANG_GN    "gn"     /* Guarani */
#define HPDF_LANG_GU    "gu"     /* Gujarati */
#define HPDF_LANG_HA    "ha"     /* Hausa */
#define HPDF_LANG_HI    "hi"     /* Hindi */
#define HPDF_LANG_HR    "hr"     /* Croatian */
#define HPDF_LANG_HU    "hu"     /* Hungarian */
#define HPDF_LANG_HY    "hy"     /* Armenian */
#define HPDF_LANG_IA    "ia"     /* Interlingua */
#define HPDF_LANG_IE    "ie"     /* Interlingue */
#define HPDF_LANG_IK    "ik"     /* Inupiak */
#define HPDF_LANG_IN    "in"     /* Indonesian */
#define HPDF_LANG_IS    "is"     /* Icelandic */
#define HPDF_LANG_IT    "it"     /* Italian */
#define HPDF_LANG_IW    "iw"     /* Hebrew */
#define HPDF_LANG_JA    "ja"     /* Japanese */
#define HPDF_LANG_JI    "ji"     /* Yiddish */
#define HPDF_LANG_JW    "jw"     /* Javanese */
#define HPDF_LANG_KA    "ka"     /* Georgian */
#define HPDF_LANG_KK    "kk"     /* Kazakh */
#define HPDF_LANG_KL    "kl"     /* Greenlandic */
#define HPDF_LANG_KM    "km"     /* Cambodian */
#define HPDF_LANG_KN    "kn"     /* Kannada */
#define HPDF_LANG_KO    "ko"     /* Korean */
#define HPDF_LANG_KS    "ks"     /* Kashmiri */
#define HPDF_LANG_KU    "ku"     /* Kurdish */
#define HPDF_LANG_KY    "ky"     /* Kirghiz */
#define HPDF_LANG_LA    "la"     /* Latin */
#define HPDF_LANG_LN    "ln"     /* Lingala */
#define HPDF_LANG_LO    "lo"     /* Laothian */
#define HPDF_LANG_LT    "lt"     /* Lithuanian */
#define HPDF_LANG_LV    "lv"     /* Latvian,Lettish */
#define HPDF_LANG_MG    "mg"     /* Malagasy */
#define HPDF_LANG_MI    "mi"     /* Maori */
#define HPDF_LANG_MK    "mk"     /* Macedonian */
#define HPDF_LANG_ML    "ml"     /* Malayalam */
#define HPDF_LANG_MN    "mn"     /* Mongolian */
#define HPDF_LANG_MO    "mo"     /* Moldavian */
#define HPDF_LANG_MR    "mr"     /* Marathi */
#define HPDF_LANG_MS    "ms"     /* Malay */
#define HPDF_LANG_MT    "mt"     /* Maltese */
#define HPDF_LANG_MY    "my"     /* Burmese */
#define HPDF_LANG_NA    "na"     /* Nauru */
#define HPDF_LANG_NE    "ne"     /* Nepali */
#define HPDF_LANG_NL    "nl"     /* Dutch */
#define HPDF_LANG_NO    "no"     /* Norwegian */
#define HPDF_LANG_OC    "oc"     /* Occitan */
#define HPDF_LANG_OM    "om"     /* (Afan)Oromo */
#define HPDF_LANG_OR    "or"     /* Oriya */
#define HPDF_LANG_PA    "pa"     /* Punjabi */
#define HPDF_LANG_PL    "pl"     /* Polish */
#define HPDF_LANG_PS    "ps"     /* Pashto,Pushto */
#define HPDF_LANG_PT    "pt"     /* Portuguese  */
#define HPDF_LANG_QU    "qu"     /* Quechua */
#define HPDF_LANG_RM    "rm"     /* Rhaeto-Romance */
#define HPDF_LANG_RN    "rn"     /* Kirundi */
#define HPDF_LANG_RO    "ro"     /* Romanian */
#define HPDF_LANG_RU    "ru"     /* Russian */
#define HPDF_LANG_RW    "rw"     /* Kinyarwanda */
#define HPDF_LANG_SA    "sa"     /* Sanskrit */
#define HPDF_LANG_SD    "sd"     /* Sindhi */
#define HPDF_LANG_SG    "sg"     /* Sangro */
#define HPDF_LANG_SH    "sh"     /* Serbo-Croatian */
#define HPDF_LANG_SI    "si"     /* Singhalese */
#define HPDF_LANG_SK    "sk"     /* Slovak */
#define HPDF_LANG_SL    "sl"     /* Slovenian */
#define HPDF_LANG_SM    "sm"     /* Samoan */
#define HPDF_LANG_SN    "sn"     /* Shona */
#define HPDF_LANG_SO    "so"     /* Somali */
#define HPDF_LANG_SQ    "sq"     /* Albanian */
#define HPDF_LANG_SR    "sr"     /* Serbian */
#define HPDF_LANG_SS    "ss"     /* Siswati */
#define HPDF_LANG_ST    "st"     /* Sesotho */
#define HPDF_LANG_SU    "su"     /* Sundanese */
#define HPDF_LANG_SV    "sv"     /* Swedish */
#define HPDF_LANG_SW    "sw"     /* Swahili */
#define HPDF_LANG_TA    "ta"     /* Tamil */
#define HPDF_LANG_TE    "te"     /* Tegulu */
#define HPDF_LANG_TG    "tg"     /* Tajik */
#define HPDF_LANG_TH    "th"     /* Thai */
#define HPDF_LANG_TI    "ti"     /* Tigrinya */
#define HPDF_LANG_TK    "tk"     /* Turkmen */
#define HPDF_LANG_TL    "tl"     /* Tagalog */
#define HPDF_LANG_TN    "tn"     /* Setswanato Tonga */
#define HPDF_LANG_TR    "tr"     /* Turkish */
#define HPDF_LANG_TS    "ts"     /* Tsonga */
#define HPDF_LANG_TT    "tt"     /* Tatar */
#define HPDF_LANG_TW    "tw"     /* Twi */
#define HPDF_LANG_UK    "uk"     /* Ukrainian */
#define HPDF_LANG_UR    "ur"     /* Urdu */
#define HPDF_LANG_UZ    "uz"     /* Uzbek */
#define HPDF_LANG_VI    "vi"     /* Vietnamese */
#define HPDF_LANG_VO    "vo"     /* Volapuk */
#define HPDF_LANG_WO    "wo"     /* Wolof */
#define HPDF_LANG_XH    "xh"     /* Xhosa */
#define HPDF_LANG_YO    "yo"     /* Yoruba */
#define HPDF_LANG_ZH    "zh"     /* Chinese */
#define HPDF_LANG_ZU    "zu"     /* Zulu */


/*----------------------------------------------------------------------------*/
/*----- Graphis mode ---------------------------------------------------------*/

#define   HPDF_GMODE_PAGE_DESCRIPTION       0x0001
#define   HPDF_GMODE_PATH_OBJECT            0x0002
#define   HPDF_GMODE_TEXT_OBJECT            0x0004
#define   HPDF_GMODE_CLIPPING_PATH          0x0008
#define   HPDF_GMODE_SHADING                0x0010
#define   HPDF_GMODE_INLINE_IMAGE           0x0020
#define   HPDF_GMODE_EXTERNAL_OBJECT        0x0040


/*----------------------------------------------------------------------------*/

#endif /* _HPDF_CONSTS_H */
