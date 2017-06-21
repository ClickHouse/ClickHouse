Settings
========

In this section, we review settings that you can make using a SET query or in a config file.
Remember that these settings can be set for a session or globally.
Settings that can only be made in the server config file are not covered here.


There are three ways to setup settings (sorted by priority):

* Settings in server configuration files.

    They are set in user profiles.

* Session-level settings.

    Use ``SET setting=value`` command in ClickHouse console client running in interactive mode.
    Also sessions are supported in HTTP interface (you need to pass ``session_id`` HTTP parameter)

* Query-level settings.

    Use ``--setting=value`` command line parameters in non-iteractive mode of ClickHouse console client.
    Use HTTP parameters (``URL?setting_1=value&setting_2=value...``) with HTTP ClickHouse interface.


.. toctree::
    :glob:

    *
