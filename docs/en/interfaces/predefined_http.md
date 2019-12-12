# Predefined HTTP Interface {#predefined_http_interface}

ClickHouse already supports specific queries through the [HTTP interface](../../interfaces/HTTP/http.md) . For example, you can write data to a table as follows:
 
```bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

Now ClickHouse supports Predefined HTTP Interface which can help you more easy integration with third party tools. For example, the following example describes how to by Prometheus with Predefined HTTP Interface integration.

> Example:
> 
> * First, add these configurations to the configuration file:
> 
> ``` xml
> <http_handlers>
>	<predefine_query_handler>
>	    <url>/metrics</url>
>	    <method>GET</method>
>	    <queries>
>	        <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
>	    </queries>
>	</predefine_query_handler>
> </http_handlers>
> ```
> 
> * You can now request the url directly for data in the Prometheus format:
> 
> ``` bash
> curl -vvv 'http://localhost:8123/metrics'
> *   Trying ::1...
> * Connected to localhost (::1) port 8123 (#0)
> > GET /metrics HTTP/1.1
> > Host: localhost:8123
> > User-Agent: curl/7.47.0
> > Accept: */*
> > 
> < HTTP/1.1 200 OK
> < Date: Wed, 27 Nov 2019 08:54:25 GMT
> < Connection: Keep-Alive
> < Content-Type: text/plain; charset=UTF-8
> < X-ClickHouse-Server-Display-Name: i-tl62qd0o
> < Transfer-Encoding: chunked
> < X-ClickHouse-Query-Id: f39235f6-6ed7-488c-ae07-c7ceafb960f6
> < Keep-Alive: timeout=3
> < X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
> < 
> # HELP "Query" "Number of executing queries"
> # TYPE "Query" counter
> "Query" 1
> 
> # HELP "Merge" "Number of executing background merges"
> # TYPE "Merge" counter
> "Merge" 0
> 
> # HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
> # TYPE "PartMutation" counter
> "PartMutation" 0
> 
> # HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
> # TYPE "ReplicatedFetch" counter
> "ReplicatedFetch" 0
> 
> # HELP "ReplicatedSend" "Number of data parts being sent to replicas"
> # TYPE "ReplicatedSend" counter
> "ReplicatedSend" 0
> 
> * Connection #0 to host localhost left intact
> ```

As you can see from the example, if  `<http_handlers>` is configured in the configuration file, ClickHouse will match the HTTP requests received to the predefined type in  `<http_handlers>`, then ClickHouse will execute the corresponding predefined query if the match is successful.

Now `<http_handlers>` can configure `<root_handler>`, `<ping_handler>`, `<replicas_status_handler>`, `<dynamic_query_handler>` and `<no_handler_description>` .

## root_handler

 `<root_handler>` returns the specified content for the root path request. The specific return content is configured by `http_server_default_response` in config.xml. if not specified, return **Ok.** 

`http_server_default_response` is not defined and an HTTP request is sent to ClickHouse. The result is as follows:

```xml
<http_handlers>
    <root_handler/>
</http_handlers>
```

```
$ curl 'http://localhost:8123'
Ok.
```

`http_server_default_response` is defined and an HTTP request is sent to ClickHouse. The result is as follows:

```xml
<http_server_default_response><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></http_server_default_response>

<http_handlers>
    <root_handler/>
</http_handlers>
```

```
$ curl 'http://localhost:8123'
<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>% 
```

## ping_handler

`<ping_handler>` can be used to probe the health of the current ClickHouse Server. When the ClickHouse HTTP interface is normal, accessing ClickHouse through `<ping_handler>` will return **Ok.**.

Example:

```xml
<http_handlers>
    <ping_handler>/ping</ping_handler>
</http_handlers>
```

```bash
$ curl 'http://localhost:8123/ping'
Ok.
```

## replicas_status_handler

`<replicas_status_handler>` is used to detect the state of the replica node and return **Ok.** if the replica node has no delay. If there is a delay, return the specific delay. The value of `<replicas_status_handler>`  supports customization. If you do not specify `<replicas_status_handler>`, ClickHouse default setting `<replicas_status_handler>` is **/replicas_status**.

Example:

```xml
<http_handlers>
    <replicas_status_handler>/replicas_status</replicas_status_handler>
</http_handlers>
```

```bash
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

## predefined_query_handler

You can configure `<method>`, `<headers>`, `<url>` and `<queries>` in `<predefined_query_handler>`.

`<method>` is responsible for matching the method part of the HTTP request. `<method>` fully conforms to the definition of method in the HTTP protocol. It is an optional configuration.

`<url>`  is responsible for matching the url part of the HTTP request. It is compatible with RE2's regular expressions. It is an optional configuration.

`<headers>` is responsible for matching the header part of the HTTP request. It is compatible with RE2's regular expressions. It is an optional configuration.

`<queries>` value is a predefined query of `<predefined_query_handler>`, which is executed by ClickHouse when an HTTP request is matched and the result of the query is returned. It is a must configuration.

`<predefined_query_handler>` supports setting Settings and query_params values. 

To experiment with this functionality, the example defines the values of max_threads and max_alter_threads and queries whether the Settings were set successfully.

Example:

```xml
<root_handlers>
	<predefined_query_handler>
	    <method>GET</method>
	    <headers>
	        <XXX>TEST_HEADER_VALUE</XXX>
	        <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
	    </headers>
	    <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
	    <queries>
	        <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
	        <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
	    </queries>
	</predefined_query_handler>
</root_handlers>
```

```bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads	2
```

> Note:
>
> In one `<predefined_query_handler>`, one `<queries>` only supports one `<query>` of an insert type.

## dynamic_query_handler

`<dynamic_query_handler>` than `<predefined_query_handler>` increased  `<query_param_name>` .

ClickHouse extracts and executes the value corresponding to the `<query_param_name>` value in the url of the HTTP request.
ClickHouse default setting `<query_param_name>` is `/query` . It is an optional configuration.

To experiment with this functionality, the example defines the values of max_threads and max_alter_threads and queries whether the Settings were set successfully.


Example:

```xml
<root_handlers>
	<dynamic_query_handler>
	    <headers>
	        <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>
	        <PARAMS_XXX><![CDATA[(?P<param_name_1>[^/]+)(/(?P<param_name_2>[^/]+))?]]></PARAMS_XXX>
	    </headers>
	    <query_param_name>query_param</query_param_name>
	</dynamic_query_handler>
</root_handlers>
```

```bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/?query_param=SELECT%20value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D&max_threads=1&max_alter_threads=2&param_name_2=max_alter_threads'
1
2
```

[Original article](https://clickhouse.yandex/docs/en/interfaces/predefined_http_interface/) <!--hide-->
