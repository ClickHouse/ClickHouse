---
sidebar_position: 39
sidebar_label: Internal Dictionaries
---

# Internal Dictionaries

ClickHouse contains a built-in feature for working with a geobase.

This allows you to:

-   Use a region’s ID to get its name in the desired language.
-   Use a region’s ID to get the ID of a city, area, federal district, country, or continent.
-   Check whether a region is part of another region.
-   Get a chain of parent regions.

All the functions support “translocality,” the ability to simultaneously use different perspectives on region ownership. For more information, see the section “Functions for working with web analytics dictionaries”.

The internal dictionaries are disabled in the default package.
To enable them, uncomment the parameters `path_to_regions_hierarchy_file` and `path_to_regions_names_files` in the server configuration file.

The geobase is loaded from text files.

Place the `regions_hierarchy*.txt` files into the `path_to_regions_hierarchy_file` directory. This configuration parameter must contain the path to the `regions_hierarchy.txt` file (the default regional hierarchy), and the other files (`regions_hierarchy_ua.txt`) must be located in the same directory.

Put the `regions_names_*.txt` files in the `path_to_regions_names_files` directory.

You can also create these files yourself. The file format is as follows:

`regions_hierarchy*.txt`: TabSeparated (no header), columns:

-   region ID (`UInt32`)
-   parent region ID (`UInt32`)
-   region type (`UInt8`): 1 - continent, 3 - country, 4 - federal district, 5 - region, 6 - city; other types do not have values
-   population (`UInt32`) — optional column

`regions_names_*.txt`: TabSeparated (no header), columns:

-   region ID (`UInt32`)
-   region name (`String`) — Can’t contain tabs or line feeds, even escaped ones.

A flat array is used for storing in RAM. For this reason, IDs shouldn’t be more than a million.

Dictionaries can be updated without restarting the server. However, the set of available dictionaries is not updated.
For updates, the file modification times are checked. If a file has changed, the dictionary is updated.
The interval to check for changes is configured in the `builtin_dictionaries_reload_interval` parameter.
Dictionary updates (other than loading at first use) do not block queries. During updates, queries use the old versions of dictionaries. If an error occurs during an update, the error is written to the server log, and queries continue using the old version of dictionaries.

We recommend periodically updating the dictionaries with the geobase. During an update, generate new files and write them to a separate location. When everything is ready, rename them to the files used by the server.

There are also functions for working with OS identifiers and search engines, but they shouldn’t be used.

