# PrettyNoEscapes

Differs from Pretty in that ANSI-escape sequences aren't used. This is necessary for displaying this format in a browser, as well as for using the 'watch' command-line utility.

Example:

```bash
watch -n1 "clickhouse-client --query='SELECT * FROM system.events FORMAT PrettyCompactNoEscapes'"
```

You can use the HTTP interface for displaying in the browser.

## PrettyCompactNoEscapes

The same as the previous setting.

## PrettySpaceNoEscapes

The same as the previous setting.

