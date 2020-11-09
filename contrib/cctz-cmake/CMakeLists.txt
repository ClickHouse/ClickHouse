option (USE_INTERNAL_CCTZ "Use internal cctz library" ${NOT_UNBUNDLED})

if (USE_INTERNAL_CCTZ)
    SET(LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/cctz)

    SET (SRCS
        ${LIBRARY_DIR}/src/civil_time_detail.cc
        ${LIBRARY_DIR}/src/time_zone_fixed.cc
        ${LIBRARY_DIR}/src/time_zone_format.cc
        ${LIBRARY_DIR}/src/time_zone_if.cc
        ${LIBRARY_DIR}/src/time_zone_impl.cc
        ${LIBRARY_DIR}/src/time_zone_info.cc
        ${LIBRARY_DIR}/src/time_zone_libc.cc
        ${LIBRARY_DIR}/src/time_zone_lookup.cc
        ${LIBRARY_DIR}/src/time_zone_posix.cc
        ${LIBRARY_DIR}/src/zone_info_source.cc
    )

    add_library (cctz ${SRCS})
    target_include_directories (cctz PUBLIC ${LIBRARY_DIR}/include)

    if (OS_FREEBSD)
        # yes, need linux, because bsd check inside linux in time_zone_libc.cc:24
        target_compile_definitions (cctz PRIVATE __USE_BSD linux _XOPEN_SOURCE=600)
    endif ()

    # Build a libray with embedded tzdata

    if (OS_LINUX AND ARCH_AMD64)

        find_program (OBJCOPY_PATH NAMES "llvm-objcopy" "llvm-objcopy-10" "llvm-objcopy-9" "llvm-objcopy-8" "objcopy")
        if (OBJCOPY_PATH)
            message(STATUS "Using objcopy: ${OBJCOPY_PATH}.")
        else ()
            message(FATAL_ERROR "Cannot find objcopy.")
        endif ()

        set (TIMEZONES
            Africa/Abidjan
            Africa/Accra
            Africa/Addis_Ababa
            Africa/Algiers
            Africa/Asmara
            Africa/Asmera
            Africa/Bamako
            Africa/Bangui
            Africa/Banjul
            Africa/Bissau
            Africa/Blantyre
            Africa/Brazzaville
            Africa/Bujumbura
            Africa/Cairo
            Africa/Casablanca
            Africa/Ceuta
            Africa/Conakry
            Africa/Dakar
            Africa/Dar_es_Salaam
            Africa/Djibouti
            Africa/Douala
            Africa/El_Aaiun
            Africa/Freetown
            Africa/Gaborone
            Africa/Harare
            Africa/Johannesburg
            Africa/Juba
            Africa/Kampala
            Africa/Khartoum
            Africa/Kigali
            Africa/Kinshasa
            Africa/Lagos
            Africa/Libreville
            Africa/Lome
            Africa/Luanda
            Africa/Lubumbashi
            Africa/Lusaka
            Africa/Malabo
            Africa/Maputo
            Africa/Maseru
            Africa/Mbabane
            Africa/Mogadishu
            Africa/Monrovia
            Africa/Nairobi
            Africa/Ndjamena
            Africa/Niamey
            Africa/Nouakchott
            Africa/Ouagadougou
            Africa/Porto-Novo
            Africa/Sao_Tome
            Africa/Timbuktu
            Africa/Tripoli
            Africa/Tunis
            Africa/Windhoek
            America/Adak
            America/Anchorage
            America/Anguilla
            America/Antigua
            America/Araguaina
            America/Argentina/Buenos_Aires
            America/Argentina/Catamarca
            America/Argentina/ComodRivadavia
            America/Argentina/Cordoba
            America/Argentina/Jujuy
            America/Argentina/La_Rioja
            America/Argentina/Mendoza
            America/Argentina/Rio_Gallegos
            America/Argentina/Salta
            America/Argentina/San_Juan
            America/Argentina/San_Luis
            America/Argentina/Tucuman
            America/Argentina/Ushuaia
            America/Aruba
            America/Asuncion
            America/Atikokan
            America/Atka
            America/Bahia
            America/Bahia_Banderas
            America/Barbados
            America/Belem
            America/Belize
            America/Blanc-Sablon
            America/Boa_Vista
            America/Bogota
            America/Boise
            America/Buenos_Aires
            America/Cambridge_Bay
            America/Campo_Grande
            America/Cancun
            America/Caracas
            America/Catamarca
            America/Cayenne
            America/Cayman
            America/Chicago
            America/Chihuahua
            America/Coral_Harbour
            America/Cordoba
            America/Costa_Rica
            America/Creston
            America/Cuiaba
            America/Curacao
            America/Danmarkshavn
            America/Dawson
            America/Dawson_Creek
            America/Denver
            America/Detroit
            America/Dominica
            America/Edmonton
            America/Eirunepe
            America/El_Salvador
            America/Ensenada
            America/Fortaleza
            America/Fort_Nelson
            America/Fort_Wayne
            America/Glace_Bay
            America/Godthab
            America/Goose_Bay
            America/Grand_Turk
            America/Grenada
            America/Guadeloupe
            America/Guatemala
            America/Guayaquil
            America/Guyana
            America/Halifax
            America/Havana
            America/Hermosillo
            America/Indiana/Indianapolis
            America/Indiana/Knox
            America/Indiana/Marengo
            America/Indiana/Petersburg
            America/Indianapolis
            America/Indiana/Tell_City
            America/Indiana/Vevay
            America/Indiana/Vincennes
            America/Indiana/Winamac
            America/Inuvik
            America/Iqaluit
            America/Jamaica
            America/Jujuy
            America/Juneau
            America/Kentucky/Louisville
            America/Kentucky/Monticello
            America/Knox_IN
            America/Kralendijk
            America/La_Paz
            America/Lima
            America/Los_Angeles
            America/Louisville
            America/Lower_Princes
            America/Maceio
            America/Managua
            America/Manaus
            America/Marigot
            America/Martinique
            America/Matamoros
            America/Mazatlan
            America/Mendoza
            America/Menominee
            America/Merida
            America/Metlakatla
            America/Mexico_City
            America/Miquelon
            America/Moncton
            America/Monterrey
            America/Montevideo
            America/Montreal
            America/Montserrat
            America/Nassau
            America/New_York
            America/Nipigon
            America/Nome
            America/Noronha
            America/North_Dakota/Beulah
            America/North_Dakota/Center
            America/North_Dakota/New_Salem
            America/Ojinaga
            America/Panama
            America/Pangnirtung
            America/Paramaribo
            America/Phoenix
            America/Port-au-Prince
            America/Porto_Acre
            America/Port_of_Spain
            America/Porto_Velho
            America/Puerto_Rico
            America/Punta_Arenas
            America/Rainy_River
            America/Rankin_Inlet
            America/Recife
            America/Regina
            America/Resolute
            America/Rio_Branco
            America/Rosario
            America/Santa_Isabel
            America/Santarem
            America/Santiago
            America/Santo_Domingo
            America/Sao_Paulo
            America/Scoresbysund
            America/Shiprock
            America/Sitka
            America/St_Barthelemy
            America/St_Johns
            America/St_Kitts
            America/St_Lucia
            America/St_Thomas
            America/St_Vincent
            America/Swift_Current
            America/Tegucigalpa
            America/Thule
            America/Thunder_Bay
            America/Tijuana
            America/Toronto
            America/Tortola
            America/Vancouver
            America/Virgin
            America/Whitehorse
            America/Winnipeg
            America/Yakutat
            America/Yellowknife
            Antarctica/Casey
            Antarctica/Davis
            Antarctica/DumontDUrville
            Antarctica/Macquarie
            Antarctica/Mawson
            Antarctica/McMurdo
            Antarctica/Palmer
            Antarctica/Rothera
            Antarctica/South_Pole
            Antarctica/Syowa
            Antarctica/Troll
            Antarctica/Vostok
            Arctic/Longyearbyen
            Asia/Aden
            Asia/Almaty
            Asia/Amman
            Asia/Anadyr
            Asia/Aqtau
            Asia/Aqtobe
            Asia/Ashgabat
            Asia/Ashkhabad
            Asia/Atyrau
            Asia/Baghdad
            Asia/Bahrain
            Asia/Baku
            Asia/Bangkok
            Asia/Barnaul
            Asia/Beirut
            Asia/Bishkek
            Asia/Brunei
            Asia/Calcutta
            Asia/Chita
            Asia/Choibalsan
            Asia/Chongqing
            Asia/Chungking
            Asia/Colombo
            Asia/Dacca
            Asia/Damascus
            Asia/Dhaka
            Asia/Dili
            Asia/Dubai
            Asia/Dushanbe
            Asia/Famagusta
            Asia/Gaza
            Asia/Harbin
            Asia/Hebron
            Asia/Ho_Chi_Minh
            Asia/Hong_Kong
            Asia/Hovd
            Asia/Irkutsk
            Asia/Istanbul
            Asia/Jakarta
            Asia/Jayapura
            Asia/Jerusalem
            Asia/Kabul
            Asia/Kamchatka
            Asia/Karachi
            Asia/Kashgar
            Asia/Kathmandu
            Asia/Katmandu
            Asia/Khandyga
            Asia/Kolkata
            Asia/Krasnoyarsk
            Asia/Kuala_Lumpur
            Asia/Kuching
            Asia/Kuwait
            Asia/Macao
            Asia/Macau
            Asia/Magadan
            Asia/Makassar
            Asia/Manila
            Asia/Muscat
            Asia/Nicosia
            Asia/Novokuznetsk
            Asia/Novosibirsk
            Asia/Omsk
            Asia/Oral
            Asia/Phnom_Penh
            Asia/Pontianak
            Asia/Pyongyang
            Asia/Qatar
            Asia/Qostanay
            Asia/Qyzylorda
            Asia/Rangoon
            Asia/Riyadh
            Asia/Saigon
            Asia/Sakhalin
            Asia/Samarkand
            Asia/Seoul
            Asia/Shanghai
            Asia/Singapore
            Asia/Srednekolymsk
            Asia/Taipei
            Asia/Tashkent
            Asia/Tbilisi
            Asia/Tehran
            Asia/Tel_Aviv
            Asia/Thimbu
            Asia/Thimphu
            Asia/Tokyo
            Asia/Tomsk
            Asia/Ujung_Pandang
            Asia/Ulaanbaatar
            Asia/Ulan_Bator
            Asia/Urumqi
            Asia/Ust-Nera
            Asia/Vientiane
            Asia/Vladivostok
            Asia/Yakutsk
            Asia/Yangon
            Asia/Yekaterinburg
            Asia/Yerevan
            Atlantic/Azores
            Atlantic/Bermuda
            Atlantic/Canary
            Atlantic/Cape_Verde
            Atlantic/Faeroe
            Atlantic/Faroe
            Atlantic/Jan_Mayen
            Atlantic/Madeira
            Atlantic/Reykjavik
            Atlantic/South_Georgia
            Atlantic/Stanley
            Atlantic/St_Helena
            Australia/ACT
            Australia/Adelaide
            Australia/Brisbane
            Australia/Broken_Hill
            Australia/Canberra
            Australia/Currie
            Australia/Darwin
            Australia/Eucla
            Australia/Hobart
            Australia/LHI
            Australia/Lindeman
            Australia/Lord_Howe
            Australia/Melbourne
            Australia/North
            Australia/NSW
            Australia/Perth
            Australia/Queensland
            Australia/South
            Australia/Sydney
            Australia/Tasmania
            Australia/Victoria
            Australia/West
            Australia/Yancowinna
            Brazil/Acre
            Brazil/DeNoronha
            Brazil/East
            Brazil/West
            Canada/Atlantic
            Canada/Central
            Canada/Eastern
            Canada/Mountain
            Canada/Newfoundland
            Canada/Pacific
            Canada/Saskatchewan
            Canada/Yukon
            CET
            Chile/Continental
            Chile/EasterIsland
            CST6CDT
            Cuba
            EET
            Egypt
            Eire
            EST
            EST5EDT
            Etc/GMT
            Etc/Greenwich
            Etc/UCT
            Etc/Universal
            Etc/UTC
            Etc/Zulu
            Europe/Amsterdam
            Europe/Andorra
            Europe/Astrakhan
            Europe/Athens
            Europe/Belfast
            Europe/Belgrade
            Europe/Berlin
            Europe/Bratislava
            Europe/Brussels
            Europe/Bucharest
            Europe/Budapest
            Europe/Busingen
            Europe/Chisinau
            Europe/Copenhagen
            Europe/Dublin
            Europe/Gibraltar
            Europe/Guernsey
            Europe/Helsinki
            Europe/Isle_of_Man
            Europe/Istanbul
            Europe/Jersey
            Europe/Kaliningrad
            Europe/Kiev
            Europe/Kirov
            Europe/Lisbon
            Europe/Ljubljana
            Europe/London
            Europe/Luxembourg
            Europe/Madrid
            Europe/Malta
            Europe/Mariehamn
            Europe/Minsk
            Europe/Monaco
            Europe/Moscow
            Europe/Nicosia
            Europe/Oslo
            Europe/Paris
            Europe/Podgorica
            Europe/Prague
            Europe/Riga
            Europe/Rome
            Europe/Samara
            Europe/San_Marino
            Europe/Sarajevo
            Europe/Saratov
            Europe/Simferopol
            Europe/Skopje
            Europe/Sofia
            Europe/Stockholm
            Europe/Tallinn
            Europe/Tirane
            Europe/Tiraspol
            Europe/Ulyanovsk
            Europe/Uzhgorod
            Europe/Vaduz
            Europe/Vatican
            Europe/Vienna
            Europe/Vilnius
            Europe/Volgograd
            Europe/Warsaw
            Europe/Zagreb
            Europe/Zaporozhye
            Europe/Zurich
            Factory
            GB
            GB-Eire
            GMT
            GMT0
            Greenwich
            Hongkong
            HST
            Iceland
            Indian/Antananarivo
            Indian/Chagos
            Indian/Christmas
            Indian/Cocos
            Indian/Comoro
            Indian/Kerguelen
            Indian/Mahe
            Indian/Maldives
            Indian/Mauritius
            Indian/Mayotte
            Indian/Reunion
            Iran
            Israel
            Jamaica
            Japan
            Kwajalein
            Libya
            MET
            Mexico/BajaNorte
            Mexico/BajaSur
            Mexico/General
            MST
            MST7MDT
            Navajo
            NZ
            NZ-CHAT
            Pacific/Apia
            Pacific/Auckland
            Pacific/Bougainville
            Pacific/Chatham
            Pacific/Chuuk
            Pacific/Easter
            Pacific/Efate
            Pacific/Enderbury
            Pacific/Fakaofo
            Pacific/Fiji
            Pacific/Funafuti
            Pacific/Galapagos
            Pacific/Gambier
            Pacific/Guadalcanal
            Pacific/Guam
            Pacific/Honolulu
            Pacific/Johnston
            Pacific/Kiritimati
            Pacific/Kosrae
            Pacific/Kwajalein
            Pacific/Majuro
            Pacific/Marquesas
            Pacific/Midway
            Pacific/Nauru
            Pacific/Niue
            Pacific/Norfolk
            Pacific/Noumea
            Pacific/Pago_Pago
            Pacific/Palau
            Pacific/Pitcairn
            Pacific/Pohnpei
            Pacific/Ponape
            Pacific/Port_Moresby
            Pacific/Rarotonga
            Pacific/Saipan
            Pacific/Samoa
            Pacific/Tahiti
            Pacific/Tarawa
            Pacific/Tongatapu
            Pacific/Truk
            Pacific/Wake
            Pacific/Wallis
            Pacific/Yap
            Poland
            Portugal
            PRC
            PST8PDT
            ROC
            ROK
            Singapore
            Turkey
            UCT
            Universal
            US/Alaska
            US/Aleutian
            US/Arizona
            US/Central
            US/Eastern
            US/East-Indiana
            US/Hawaii
            US/Indiana-Starke
            US/Michigan
            US/Mountain
            US/Pacific
            US/Samoa
            UTC
            WET
            W-SU
            Zulu)

        set(TZDIR ${LIBRARY_DIR}/testdata/zoneinfo)
        set(TZ_OBJS)

        foreach(TIMEZONE ${TIMEZONES})
            string(REPLACE "/" "_" TIMEZONE_ID ${TIMEZONE})
            set(TZ_OBJ ${TIMEZONE_ID}.o)
            set(TZ_OBJS ${TZ_OBJS} ${TZ_OBJ})

            # https://stackoverflow.com/questions/14776463/compile-and-add-an-object-file-from-a-binary-with-cmake
            add_custom_command(OUTPUT ${TZ_OBJ}
                COMMAND cd ${TZDIR} && ${OBJCOPY_PATH} -I binary -O elf64-x86-64 -B i386 ${TIMEZONE} ${CMAKE_CURRENT_BINARY_DIR}/${TZ_OBJ}
                COMMAND ${OBJCOPY_PATH} --rename-section .data=.rodata,alloc,load,readonly,data,contents
                    ${CMAKE_CURRENT_BINARY_DIR}/${TZ_OBJ} ${CMAKE_CURRENT_BINARY_DIR}/${TZ_OBJ})

            set_source_files_properties(${TZ_OBJ} PROPERTIES EXTERNAL_OBJECT true GENERATED true)
        endforeach(TIMEZONE)

        add_library(tzdata STATIC ${TZ_OBJS})
        set_target_properties(tzdata PROPERTIES LINKER_LANGUAGE C)
        # whole-archive prevents symbols from being discarded for unknown reason
        # CMake can shuffle each of target_link_libraries arguments with other
        # libraries in linker command. To avoid this we hardcode whole-archive
        # library into single string.
        add_dependencies(cctz tzdata)
        target_link_libraries(cctz INTERFACE "-Wl,--whole-archive $<TARGET_FILE:tzdata> -Wl,--no-whole-archive")
    endif ()

else ()
    find_library (LIBRARY_CCTZ cctz)
    find_path (INCLUDE_CCTZ NAMES cctz/civil_time.h)

    add_library (cctz UNKNOWN IMPORTED)
    set_property (TARGET cctz PROPERTY IMPORTED_LOCATION ${LIBRARY_CCTZ})
    set_property (TARGET cctz PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${INCLUDE_CCTZ})

    set(CMAKE_REQUIRED_LIBRARIES cctz)
    check_cxx_source_compiles(
        "
        #include <cctz/civil_time.h>
        int main() {
            cctz::civil_day date;
        }
        "
        EXTERNAL_CCTZ_WORKS
    )

    if (NOT EXTERNAL_CCTZ_WORKS)
        message (FATAL_ERROR "cctz is unusable: ${LIBRARY_CCTZ} ${INCLUDE_CCTZ}")
    endif ()
endif ()

message (STATUS "Using cctz")
