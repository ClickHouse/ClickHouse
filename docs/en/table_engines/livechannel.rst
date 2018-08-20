LiveChannel
----------------

Used for implementing live channels (for more information, see ``CREATE LIVE CHANNEL``). It does not store any data,
but could group one or more live views into a single channel. Live view tables are added or removed from the channel
using ``ALTER CHANNEL`` command. The table does not support reading using ``SELECT``,
but only supports watching the channel using ``WATCH`` query. When watching the table,
the result of each live view is multiplexed and updated automatically when any live view result changes.
