"""
Minimal Flight SQL client using pyarrow.flight and protobuf.

Replaces the `flightsql-dbapi` package (which conflicts with pyiceberg's
SQLAlchemy>=2 requirement) with just the subset the integration tests need:
  - FlightSQLClient  (execute, execute_update, do_get, metadata commands)
  - flight_descriptor helper
  - Flight SQL protobuf message classes
"""

from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
from google.protobuf import any_pb2
from pyarrow import flight

# ---------------------------------------------------------------------------
# Protobuf definitions generated from the Arrow Flight SQL .proto
#
# The serialized FileDescriptorProto below was built programmatically from
# the Arrow Flight SQL protocol spec.  It defines:
#   CommandGetSqlInfo, CommandStatementQuery, CommandStatementUpdate,
#   DoPutUpdateResult, CommandGetCatalogs, CommandGetDbSchemas,
#   CommandGetTables, CommandGetTableTypes, CommandGetPrimaryKeys,
#   CommandStatementIngest (with nested TableDefinitionOptions + enums).
# ---------------------------------------------------------------------------
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2 as _descriptor_pb2

_sym_db = _symbol_database.Default()

_DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x19flightsql/flightsql.proto\x12\x19arrow.flight.protocol.sql'
    b'\x1a google/protobuf/descriptor.proto'
    # CommandGetSqlInfo
    b'"&\n\x11CommandGetSqlInfo\x12\x0c\n\x04info\x18\x01 \x03(\r:\x03\xc0>\x01'
    # CommandStatementQuery
    b'"[\n\x15CommandStatementQuery\x12\r\n\x05query\x18\x01 \x01(\t'
    b'\x12\x1b\n\x0etransaction_id\x18\x02 \x01(\x0cH\x00\x88\x01\x01:\x03\xc0>\x01'
    b'B\x11\n\x0f_transaction_id'
    # CommandStatementUpdate
    b'"\\\n\x16CommandStatementUpdate\x12\r\n\x05query\x18\x01 \x01(\t'
    b'\x12\x1b\n\x0etransaction_id\x18\x02 \x01(\x0cH\x00\x88\x01\x01:\x03\xc0>\x01'
    b'B\x11\n\x0f_transaction_id'
    # DoPutUpdateResult
    b'".\n\x11DoPutUpdateResult\x12\x14\n\x0crecord_count\x18\x01 \x01(\x03:\x03\xc0>\x01'
    # CommandGetCatalogs (no fields)
    b'"\x19\n\x12CommandGetCatalogs:\x03\xc0>\x01'
    # CommandGetDbSchemas
    b'"\x80\x01\n\x13CommandGetDbSchemas'
    b'\x12\x14\n\x07catalog\x18\x01 \x01(\tH\x00\x88\x01\x01'
    b'\x12%\n\x18db_schema_filter_pattern\x18\x02 \x01(\tH\x01\x88\x01\x01'
    b':\x03\xc0>\x01B\n\n\x08_catalogB\x1b\n\x19_db_schema_filter_pattern'
    # CommandGetTables
    b'"\xf0\x01\n\x10CommandGetTables'
    b'\x12\x14\n\x07catalog\x18\x01 \x01(\tH\x00\x88\x01\x01'
    b'\x12%\n\x18db_schema_filter_pattern\x18\x02 \x01(\tH\x01\x88\x01\x01'
    b'\x12&\n\x19table_name_filter_pattern\x18\x03 \x01(\tH\x02\x88\x01\x01'
    b'\x12\x13\n\x0btable_types\x18\x04 \x03(\t'
    b'\x12\x16\n\x0einclude_schema\x18\x05 \x01(\x08'
    b':\x03\xc0>\x01B\n\n\x08_catalogB\x1b\n\x19_db_schema_filter_pattern'
    b'B\x1c\n\x1a_table_name_filter_pattern'
    # CommandGetTableTypes (no fields)
    b'"\x1b\n\x14CommandGetTableTypes:\x03\xc0>\x01'
    # CommandGetPrimaryKeys
    b'"s\n\x15CommandGetPrimaryKeys'
    b'\x12\x14\n\x07catalog\x18\x01 \x01(\tH\x00\x88\x01\x01'
    b'\x12\x16\n\tdb_schema\x18\x02 \x01(\tH\x01\x88\x01\x01'
    b'\x12\r\n\x05table\x18\x03 \x01(\t'
    b':\x03\xc0>\x01B\n\n\x08_catalogB\x0c\n\n_db_schema'
    # CommandStatementIngest (with nested TableDefinitionOptions)
    b'"\xb6\x07\n\x16CommandStatementIngest'
    b'\x12j\n\x18table_definition_options\x18\x01 \x01(\x0b2H'
    b'.arrow.flight.protocol.sql.CommandStatementIngest.TableDefinitionOptions'
    b'\x12\r\n\x05table\x18\x02 \x01(\t'
    b'\x12\x13\n\x06schema\x18\x03 \x01(\tH\x00\x88\x01\x01'
    b'\x12\x14\n\x07catalog\x18\x04 \x01(\tH\x01\x88\x01\x01'
    b'\x12\x11\n\ttemporary\x18\x05 \x01(\x08'
    b'\x12\x1b\n\x0etransaction_id\x18\x06 \x01(\x0cH\x02\x88\x01\x01'
    b'\x12O\n\x07options\x18\x07 \x03(\x0b2>'
    b'.arrow.flight.protocol.sql.CommandStatementIngest.OptionsEntry'
    b'\x1a\x99\x04\n\x16TableDefinitionOptions'
    b'\x12r\n\x0cif_not_exist\x18\x01 \x01(\x0e2\\'
    b'.arrow.flight.protocol.sql.CommandStatementIngest'
    b'.TableDefinitionOptions.TableNotExistOption'
    b'\x12m\n\tif_exists\x18\x02 \x01(\x0e2Z'
    b'.arrow.flight.protocol.sql.CommandStatementIngest'
    b'.TableDefinitionOptions.TableExistsOption'
    b'"\x81\x01\n\x13TableNotExistOption'
    b'\x12&\n"TABLE_NOT_EXIST_OPTION_UNSPECIFIED\x10\x00'
    b'\x12!\n\x1dTABLE_NOT_EXIST_OPTION_CREATE\x10\x01'
    b'\x12\x1f\n\x1bTABLE_NOT_EXIST_OPTION_FAIL\x10\x02'
    b'"\x97\x01\n\x11TableExistsOption'
    b'\x12#\n\x1fTABLE_EXISTS_OPTION_UNSPECIFIED\x10\x00'
    b'\x12\x1c\n\x18TABLE_EXISTS_OPTION_FAIL\x10\x01'
    b'\x12\x1e\n\x1aTABLE_EXISTS_OPTION_APPEND\x10\x02'
    b'\x12\x1f\n\x1bTABLE_EXISTS_OPTION_REPLACE\x10\x03'
    b'\x1a*\n\x0cOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t'
    b'\x12\r\n\x05value\x18\x02 \x01(\t'
    b':\x03\xc0>\x01B\t\n\x07_schemaB\n\n\x08_catalogB\x11\n\x0f_transaction_id'
    # Extension: experimental (field 1000 on MessageOptions)
    b':6\n\x0cexperimental\x12\x1f.google.protobuf.MessageOptions\x18\xe8\x07 \x01(\x08'
    # File-level options
    b'B\x00b\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(_DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(_DESCRIPTOR, 'flightsql.flightsql_pb2', _globals)

# Expose message classes at module level.
CommandGetSqlInfo = _globals['CommandGetSqlInfo']
CommandStatementQuery = _globals['CommandStatementQuery']
CommandStatementUpdate = _globals['CommandStatementUpdate']
DoPutUpdateResult = _globals['DoPutUpdateResult']
CommandGetCatalogs = _globals['CommandGetCatalogs']
CommandGetDbSchemas = _globals['CommandGetDbSchemas']
CommandGetTables = _globals['CommandGetTables']
CommandGetTableTypes = _globals['CommandGetTableTypes']
CommandGetPrimaryKeys = _globals['CommandGetPrimaryKeys']
CommandStatementIngest = _globals['CommandStatementIngest']

# ---------------------------------------------------------------------------
# Prepared-statement messages (separate proto file to avoid conflicts).
# Defines: ActionCreatePreparedStatementRequest/Result,
#          ActionClosePreparedStatementRequest,
#          CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
#          DoPutPreparedStatementResult.
# ---------------------------------------------------------------------------
_PREP_STMT_DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1fflightsql/flightsql_extra.proto\x12\x19arrow.flight.protocol.sql'
    b'"e\n$ActionCreatePreparedStatementRequest\x12\r\n\x05query\x18\x01 \x01(\t'
    b'\x12\x1b\n\x0etransaction_id\x18\x02 \x01(\x0cH\x00\x88\x01\x01'
    b'B\x11\n\x0f_transaction_id'
    b'"z\n#ActionCreatePreparedStatementResult'
    b'\x12!\n\x19prepared_statement_handle\x18\x01 \x01(\x0c'
    b'\x12\x16\n\x0edataset_schema\x18\x02 \x01(\x0c'
    b'\x12\x18\n\x10parameter_schema\x18\x03 \x01(\x0c'
    b'"H\n#ActionClosePreparedStatementRequest'
    b'\x12!\n\x19prepared_statement_handle\x18\x01 \x01(\x0c'
    b'"B\n\x1dCommandPreparedStatementQuery'
    b'\x12!\n\x19prepared_statement_handle\x18\x01 \x01(\x0c'
    b'"C\n\x1eCommandPreparedStatementUpdate'
    b'\x12!\n\x19prepared_statement_handle\x18\x01 \x01(\x0c'
    b'"d\n\x1cDoPutPreparedStatementResult'
    b'\x12&\n\x19prepared_statement_handle\x18\x01 \x01(\x0cH\x00\x88\x01\x01'
    b'B\x1c\n\x1a_prepared_statement_handle'
    b'b\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(_PREP_STMT_DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(
    _PREP_STMT_DESCRIPTOR, 'flightsql.flightsql_extra_pb2', _globals)

ActionCreatePreparedStatementRequest = _globals['ActionCreatePreparedStatementRequest']
ActionCreatePreparedStatementResult = _globals['ActionCreatePreparedStatementResult']
ActionClosePreparedStatementRequest = _globals['ActionClosePreparedStatementRequest']
CommandPreparedStatementQuery = _globals['CommandPreparedStatementQuery']
CommandPreparedStatementUpdate = _globals['CommandPreparedStatementUpdate']
DoPutPreparedStatementResult = _globals['DoPutPreparedStatementResult']

# ---------------------------------------------------------------------------
# Flight.proto action messages (arrow.flight.protocol package)
#
# Defines: SessionOptionValue, SetSessionOptionsRequest/Result,
#          GetSessionOptionsRequest/Result, CancelFlightInfoResult,
#          CancelStatus enum.
# ---------------------------------------------------------------------------
_FLIGHT_ACTIONS_DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x1bflight/flight_actions.proto\x12\x15arrow.flight.protocol'
    # SessionOptionValue (oneof: string, bool, sfixed64, double, StringListValue)
    b'"\xfc\x01\n\x12SessionOptionValue'
    b'\x12\x16\n\x0cstring_value\x18\x01 \x01(\tH\x00'
    b'\x12\x14\n\nbool_value\x18\x02 \x01(\x08H\x00'
    b'\x12\x15\n\x0bint64_value\x18\x03 \x01(\x10H\x00'
    b'\x12\x16\n\x0cdouble_value\x18\x04 \x01(\x01H\x00'
    b'\x12V\n\x11string_list_value\x18\x05 \x01(\x0b\x32\x39'
    b'.arrow.flight.protocol.SessionOptionValue.StringListValueH\x00'
    b'\x1a!\n\x0fStringListValue\x12\x0e\n\x06values\x18\x01 \x03(\t'
    b'B\x0e\n\x0coption_value'
    # SetSessionOptionsRequest (map<string, SessionOptionValue>)
    b'"\xda\x01\n\x18SetSessionOptionsRequest'
    b'\x12\\\n\x0fsession_options\x18\x01 \x03(\x0b\x32\x43'
    b'.arrow.flight.protocol.SetSessionOptionsRequest.SessionOptionsEntry'
    b'\x1a`\n\x13SessionOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t'
    b'\x12\x38\n\x05value\x18\x02 \x01(\x0b\x32)'
    b'.arrow.flight.protocol.SessionOptionValue:\x028\x01'
    # SetSessionOptionsResult (map<string, Error>, ErrorValue enum)
    b'"\xec\x02\n\x17SetSessionOptionsResult'
    b'\x12J\n\x06\x65rrors\x18\x01 \x03(\x0b\x32:'
    b'.arrow.flight.protocol.SetSessionOptionsResult.ErrorsEntry'
    b'\x1aQ\n\x05\x45rror\x12H\n\x05value\x18\x01 \x01(\x0e\x32\x39'
    b'.arrow.flight.protocol.SetSessionOptionsResult.ErrorValue'
    b'\x1a\x63\n\x0b\x45rrorsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t'
    b'\x12\x43\n\x05value\x18\x02 \x01(\x0b\x32\x34'
    b'.arrow.flight.protocol.SetSessionOptionsResult.Error:\x028\x01'
    b'"M\n\nErrorValue\x12\x0f\n\x0bUNSPECIFIED\x10\x00'
    b'\x12\x10\n\x0cINVALID_NAME\x10\x01\x12\x11\n\rINVALID_VALUE\x10\x02'
    b'\x12\t\n\x05\x45RROR\x10\x03'
    # GetSessionOptionsRequest (empty)
    b'"\x1a\n\x18GetSessionOptionsRequest'
    # GetSessionOptionsResult (map<string, SessionOptionValue>)
    b'"\xd8\x01\n\x17GetSessionOptionsResult'
    b'\x12[\n\x0fsession_options\x18\x01 \x03(\x0b\x32\x42'
    b'.arrow.flight.protocol.GetSessionOptionsResult.SessionOptionsEntry'
    b'\x1a`\n\x13SessionOptionsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t'
    b'\x12\x38\n\x05value\x18\x02 \x01(\x0b\x32)'
    b'.arrow.flight.protocol.SessionOptionValue:\x028\x01'
    # CancelFlightInfoRequest (bytes info -- serialized FlightInfo)
    b'"\'\n\x17\x43\x61ncelFlightInfoRequest\x12\x0c\n\x04info\x18\x01 \x01(\x0c'
    # CancelFlightInfoResult (CancelStatus enum)
    b'"M\n\x16\x43\x61ncelFlightInfoResult'
    b'\x12\x33\n\x06status\x18\x01 \x01(\x0e\x32#'
    b'.arrow.flight.protocol.CancelStatus'
    # PollInfo (bytes info, bytes flight_descriptor, optional double progress)
    b'"W\n\x08PollInfo'
    b'\x12\x0c\n\x04info\x18\x01 \x01(\x0c'
    b'\x12\x19\n\x11\x66light_descriptor\x18\x02 \x01(\x0c'
    b'\x12\x15\n\x08progress\x18\x03 \x01(\x01H\x00\x88\x01\x01'
    b'B\x0b\n\t_progress'
    # CancelStatus enum
    b'*\x8b\x01\n\x0c\x43\x61ncelStatus'
    b'\x12\x1d\n\x19\x43\x41NCEL_STATUS_UNSPECIFIED\x10\x00'
    b'\x12\x1b\n\x17\x43\x41NCEL_STATUS_CANCELLED\x10\x01'
    b'\x12\x1c\n\x18\x43\x41NCEL_STATUS_CANCELLING\x10\x02'
    b'\x12!\n\x1d\x43\x41NCEL_STATUS_NOT_CANCELLABLE\x10\x03'
    b'b\x06proto3'
)

_builder.BuildMessageAndEnumDescriptors(_FLIGHT_ACTIONS_DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(
    _FLIGHT_ACTIONS_DESCRIPTOR, 'flight.flight_actions_pb2', _globals)

SessionOptionValue = _globals['SessionOptionValue']
SetSessionOptionsRequest = _globals['SetSessionOptionsRequest']
SetSessionOptionsResult = _globals['SetSessionOptionsResult']
GetSessionOptionsRequest = _globals['GetSessionOptionsRequest']
GetSessionOptionsResult = _globals['GetSessionOptionsResult']
CancelFlightInfoRequest = _globals['CancelFlightInfoRequest']
CancelFlightInfoResult = _globals['CancelFlightInfoResult']
PollInfo = _globals['PollInfo']
CancelStatus = _globals['CancelStatus']

# ---------------------------------------------------------------------------
# Thin wrapper over PollInfo to deserialize returned data
# ---------------------------------------------------------------------------
class PollResult:
    """Wraps raw PollInfo proto with pyarrow-deserialized accessors."""

    def __init__(self, proto):
        self._proto = proto

    @property
    def info(self):
        """Deserialized FlightInfo, or None."""
        if self._proto.info:
            return flight.FlightInfo.deserialize(self._proto.info)
        return None

    @property
    def flight_descriptor(self):
        """Deserialized FlightDescriptor for next poll, or None if query is complete."""
        if self._proto.flight_descriptor:
            return flight.FlightDescriptor.deserialize(self._proto.flight_descriptor)
        return None

    @property
    def progress(self):
        """Query progress 0.0-1.0, or None if unknown."""
        if self._proto.HasField('progress'):
            return self._proto.progress
        return None

    @property
    def info_bytes(self):
        """Raw serialized FlightInfo bytes for CancelFlightInfo."""
        return self._proto.info


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def flight_descriptor(command: Any) -> flight.FlightDescriptor:
    """Pack a protobuf command into a FlightDescriptor."""
    wrapper = any_pb2.Any()
    wrapper.Pack(command)
    return flight.FlightDescriptor.for_command(wrapper.SerializeToString())


def _create_flight_client(
    host: str = "localhost",
    port: int = 443,
    insecure: Optional[bool] = None,
    disable_server_verification: Optional[bool] = None,
    metadata: Optional[Dict[str, str]] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    **kwargs: Any,
) -> Tuple[flight.FlightClient, List[Tuple[bytes, bytes]]]:
    protocol = "tls"
    if insecure:
        protocol = "tcp"
    elif disable_server_verification:
        kwargs["disable_server_verification"] = True

    url = f"grpc+{protocol}://{host}:{port}"
    client = flight.FlightClient(url, **kwargs)

    headers: List[Tuple[bytes, bytes]] = []

    if username is not None:
        token = client.authenticate_basic_token(username, password or "")
        headers.append(token)

    for k, v in (metadata or {}).items():
        headers.append((k.encode("utf-8"), v.encode("utf-8")))

    return client, headers


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class FlightSQLClient:
    """
    Thin Flight SQL wrapper around pyarrow.flight.FlightClient.

    Implements only the subset used by the ClickHouse integration tests:
    execute, execute_update, do_get, and Flight SQL metadata commands,
    plus access to the underlying client.
    """

    def __init__(self, *args, features: Optional[Dict[str, str]] = None, **kwargs):
        self._host = kwargs.get('host', args[0] if args else 'localhost')
        self._port = kwargs.get('port', args[1] if len(args) > 1 else 443)
        self._insecure = kwargs.get('insecure', False)
        self._tls_root_certs = kwargs.get('tls_root_certs')
        self._override_hostname = kwargs.get('override_hostname')

        client, headers = _create_flight_client(*args, **kwargs)
        self.client = client
        self.headers = headers
        self.features = features or {}

    def _flight_call_options(self):
        headers = list(OrderedDict(self.headers).items())
        return flight.FlightCallOptions(headers=headers)

    def execute(self, query_or_stmt) -> flight.FlightInfo:
        """Execute a query string or a PreparedStatement.

        When given a string, returns FlightInfo for result retrieval.
        When given a PreparedStatement, executes it using this client's session and returns a pa.Table.
        """
        if isinstance(query_or_stmt, PreparedStatement):
            stmt = query_or_stmt
            cmd = CommandPreparedStatementQuery(prepared_statement_handle=stmt.handle)
            descriptor = flight_descriptor(cmd)
            options = self._flight_call_options()
            flight_info = self.client.get_flight_info(descriptor, options)
            tables = []
            for endpoint in flight_info.endpoints:
                reader = self.do_get(endpoint.ticket)
                tables.append(reader.read_all())
            return pa.concat_tables(tables)

        cmd = CommandStatementQuery(query=query_or_stmt)
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def execute_update(self, query: str) -> int:
        """Execute a DDL/DML statement and return the affected row count."""
        cmd = CommandStatementUpdate(query=query)
        desc = flight_descriptor(cmd)
        options = self._flight_call_options()
        writer, reader = self.client.do_put(
            desc, pa.schema([]), options
        )
        result = reader.read()
        writer.close()

        if result is None:
            return 0
        update_result = DoPutUpdateResult()
        update_result.ParseFromString(result.to_pybytes())
        return update_result.record_count

    def do_get(self, ticket) -> flight.FlightStreamReader:
        """Retrieve Arrow data for a given ticket."""
        options = self._flight_call_options()
        return self.client.do_get(ticket, options)

    # -----------------------------------------------------------------------
    # Flight SQL metadata commands
    # -----------------------------------------------------------------------

    def get_sql_info(self, info_ids: Optional[List[int]] = None) -> flight.FlightInfo:
        """Retrieve server metadata via CommandGetSqlInfo."""
        cmd = CommandGetSqlInfo()
        if info_ids:
            for info_id in info_ids:
                cmd.info.append(info_id)
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_catalogs(self) -> flight.FlightInfo:
        """Retrieve catalog list via CommandGetCatalogs."""
        cmd = CommandGetCatalogs()
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_db_schemas(self, db_schema_filter_pattern: Optional[str] = None) -> flight.FlightInfo:
        """Retrieve database schemas via CommandGetDbSchemas."""
        cmd = CommandGetDbSchemas()
        if db_schema_filter_pattern is not None:
            cmd.db_schema_filter_pattern = db_schema_filter_pattern
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_tables(
        self,
        db_schema_filter_pattern: Optional[str] = None,
        table_name_filter_pattern: Optional[str] = None,
        table_types: Optional[List[str]] = None,
        include_schema: bool = False,
    ) -> flight.FlightInfo:
        """Retrieve table list via CommandGetTables."""
        cmd = CommandGetTables()
        if db_schema_filter_pattern is not None:
            cmd.db_schema_filter_pattern = db_schema_filter_pattern
        if table_name_filter_pattern is not None:
            cmd.table_name_filter_pattern = table_name_filter_pattern
        if table_types:
            for t in table_types:
                cmd.table_types.append(t)
        cmd.include_schema = include_schema
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_table_types(self) -> flight.FlightInfo:
        """Retrieve table engine types via CommandGetTableTypes."""
        cmd = CommandGetTableTypes()
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_primary_keys(self, table: str, db_schema: Optional[str] = None) -> flight.FlightInfo:
        """Retrieve primary keys for a table via CommandGetPrimaryKeys."""
        cmd = CommandGetPrimaryKeys()
        cmd.table = table
        if db_schema is not None:
            cmd.db_schema = db_schema
        options = self._flight_call_options()
        return self.client.get_flight_info(flight_descriptor(cmd), options)

    def get_schema(self, query: str) -> flight.SchemaResult:
        """Retrieve query result schema without executing via GetSchema."""
        cmd = CommandStatementQuery(query=query)
        options = self._flight_call_options()
        return self.client.get_schema(flight_descriptor(cmd), options)

    def prepare(self, query: str) -> "PreparedStatement":
        """Create a prepared statement via the CreatePreparedStatement action."""
        req = ActionCreatePreparedStatementRequest(query=query)
        action = flight.Action("CreatePreparedStatement", req.SerializeToString())
        results = list(self.client.do_action(action, self._flight_call_options()))
        result = ActionCreatePreparedStatementResult()
        result.ParseFromString(results[0].body.to_pybytes())

        dataset_schema = None
        if result.dataset_schema:
            dataset_schema = pa.ipc.read_schema(pa.BufferReader(result.dataset_schema))

        parameter_schema = None
        if result.parameter_schema:
            parameter_schema = pa.ipc.read_schema(pa.BufferReader(result.parameter_schema))

        return PreparedStatement(
            client=self,
            handle=result.prepared_statement_handle,
            dataset_schema=dataset_schema,
            parameter_schema=parameter_schema,
        )

    def close_prepared_statement(self, handle: bytes):
        """Close a prepared statement via the ClosePreparedStatement action."""
        req = ActionClosePreparedStatementRequest(prepared_statement_handle=handle)
        action = flight.Action("ClosePreparedStatement", req.SerializeToString())
        list(self.client.do_action(action, self._flight_call_options()))

    def close_all_prepared_statements(self):
        """Close all prepared statements for the current user.

        Sends a ClosePreparedStatement action with an empty handle,
        which the server interprets as "close all".
        """
        req = ActionClosePreparedStatementRequest()
        action = flight.Action("ClosePreparedStatement", req.SerializeToString())
        list(self.client.do_action(action, self._flight_call_options()))

    def set_session_options(self, options: Dict[str, Any]) -> SetSessionOptionsResult:
        """Set session options via the SetSessionOptions action.

        Use None as a value to reset a setting to its default (sends a valueless
        SessionOptionValue, which the server interprets as SET setting = DEFAULT).
        """
        req = SetSessionOptionsRequest()
        for key, value in options.items():
            opt_val = SessionOptionValue()
            if value is None:
                pass  # leave opt_val empty — server treats this as "reset to default"
            elif isinstance(value, str):
                opt_val.string_value = value
            elif isinstance(value, bool):
                opt_val.bool_value = value
            elif isinstance(value, int):
                opt_val.int64_value = value
            elif isinstance(value, float):
                opt_val.double_value = value
            elif isinstance(value, list):
                opt_val.string_list_value.values.extend(value)
            req.session_options[key].CopyFrom(opt_val)

        action = flight.Action("SetSessionOptions", req.SerializeToString())
        results = list(self.client.do_action(action, self._flight_call_options()))
        result = SetSessionOptionsResult()
        result.ParseFromString(results[0].body.to_pybytes())
        return result

    def get_session_options(self) -> GetSessionOptionsResult:
        """Get current session options via the GetSessionOptions action."""
        req = GetSessionOptionsRequest()
        action = flight.Action("GetSessionOptions", req.SerializeToString())
        results = list(self.client.do_action(action, self._flight_call_options()))
        result = GetSessionOptionsResult()
        result.ParseFromString(results[0].body.to_pybytes())
        return result

    def cancel_flight_info(self, flight_info_bytes: bytes) -> CancelFlightInfoResult:
        """Cancel a query via the CancelFlightInfo action."""
        req = CancelFlightInfoRequest()
        req.info = flight_info_bytes
        action = flight.Action("CancelFlightInfo", req.SerializeToString())
        results = list(self.client.do_action(action, self._flight_call_options()))
        result = CancelFlightInfoResult()
        result.ParseFromString(results[0].body.to_pybytes())
        return result

    def poll_flight_info(self, descriptor) -> PollResult:
        """Call PollFlightInfo RPC via grpc.

        pyarrow.flight.FlightClient does not expose PollFlightInfo despite
        it being part of the Flight protocol and implemented in the
        underlying C++ library. We work around this by making the gRPC
        call directly, reusing the same auth headers.
        """
        import grpc as _grpc

        target = f'{self._host}:{self._port}'
        if self._insecure:
            channel = _grpc.insecure_channel(target)
        else:
            credentials = _grpc.ssl_channel_credentials(root_certificates=self._tls_root_certs)
            options = []
            if self._override_hostname:
                options.append(('grpc.ssl_target_name_override', self._override_hostname))
            channel = _grpc.secure_channel(target, credentials, options=options)

        try:
            call = channel.unary_unary(
                '/arrow.flight.protocol.FlightService/PollFlightInfo',
                request_serializer=lambda x: x,
                response_deserializer=lambda x: x,
            )
            metadata = list(OrderedDict(self.headers).items())
            raw_response = call(descriptor.serialize(), metadata=metadata, timeout=30)
        finally:
            channel.close()

        result = PollInfo()
        result.ParseFromString(raw_response)
        return PollResult(result)


class PreparedStatement:
    """Represents a server-side prepared statement."""

    def __init__(self, client: FlightSQLClient, handle: bytes,
                 dataset_schema: Optional[pa.Schema],
                 parameter_schema: Optional[pa.Schema]):
        self.client = client
        self.handle = handle
        self.dataset_schema = dataset_schema
        self.parameter_schema = parameter_schema

    def bind_parameters(self, params: pa.RecordBatch):
        """Bind parameter values via DoPut with CommandPreparedStatementQuery."""
        cmd = CommandPreparedStatementQuery(prepared_statement_handle=self.handle)
        descriptor = flight_descriptor(cmd)
        options = self.client._flight_call_options()
        writer, reader = self.client.client.do_put(descriptor, params.schema, options)
        writer.write_batch(params)
        writer.done_writing()
        result_buf = reader.read()
        writer.close()
        if result_buf:
            result = DoPutPreparedStatementResult()
            result.ParseFromString(result_buf.to_pybytes())
            if result.HasField('prepared_statement_handle'):
                self.handle = result.prepared_statement_handle

    def execute(self) -> pa.Table:
        """Execute the prepared statement query and return the result table."""
        cmd = CommandPreparedStatementQuery(prepared_statement_handle=self.handle)
        descriptor = flight_descriptor(cmd)
        options = self.client._flight_call_options()
        flight_info = self.client.client.get_flight_info(descriptor, options)
        tables = []
        for endpoint in flight_info.endpoints:
            reader = self.client.do_get(endpoint.ticket)
            tables.append(reader.read_all())
        return pa.concat_tables(tables)

    def execute_update(self) -> int:
        """Execute the prepared statement as an update (INSERT/DDL) and return affected row count."""
        cmd = CommandPreparedStatementUpdate(prepared_statement_handle=self.handle)
        descriptor = flight_descriptor(cmd)
        options = self.client._flight_call_options()

        # DoPut with CommandPreparedStatementUpdate - send empty batch
        schema = pa.schema([])
        writer, reader = self.client.client.do_put(descriptor, schema, options)
        writer.done_writing()
        result_buf = reader.read()
        writer.close()

        if result_buf:
            update_result = DoPutUpdateResult()
            update_result.ParseFromString(result_buf.to_pybytes())
            return update_result.record_count
        return 0

    def close(self):
        """Close the prepared statement on the server."""
        self.client.close_prepared_statement(self.handle)
