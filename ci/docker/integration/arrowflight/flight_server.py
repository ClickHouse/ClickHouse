#!/usr/bin/env python3

import argparse
import base64
import json

import pyarrow as pa
import pyarrow.flight as fl


class FlightServer(fl.FlightServerBase):
    def __init__(self, location, auth_handler, middleware):
        super().__init__(
            location=location, auth_handler=auth_handler, middleware=middleware
        )
        self._location = location
        self._schema = pa.schema([("column1", pa.string()), ("column2", pa.string())])
        self._schema_xyz = pa.schema([("column3", pa.int32()), ("column4", pa.int32())])
        self._tables = dict()
        self._empty_table = pa.table(
            {"column1": pa.array([]), "column2": pa.array([])}, schema=self._schema
        )
        column1_data = pa.array(["test_value_1", "abcadbc", "123456789"])
        column2_data = pa.array(["data1", "text_text_text", "data3"])
        column3_data = pa.array([1, 2, 3])
        column4_data = pa.array([4, 5, 6])
        self._tables["ABC"] = pa.table(
            {"column1": column1_data, "column2": column2_data}, schema=self._schema
        )
        self._tables["XYZ"] = pa.table(
            {"column3": column3_data, "column4": column4_data}, schema=self._schema_xyz
        )

    def do_get(self, context, ticket):
        dataset = ticket.ticket.decode()
        table = (
            self._tables[dataset] if (dataset in self._tables) else self._empty_table
        )
        return fl.RecordBatchStream(table)

    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode()
        new_data = reader.read_all()
        tables_to_concat = []
        if dataset in self._tables:
            tables_to_concat.append(self._tables[dataset])
        tables_to_concat.append(new_data.cast(target_schema=self._schema))
        self._tables[dataset] = pa.concat_tables(tables_to_concat)

    def get_schema(self, context, descriptor):
        dataset = descriptor.path[0].decode()
        if dataset in self._tables:
            return fl.SchemaResult(self._tables[dataset].schema)
        else:
            return fl.SchemaResult(self._schema)

    def do_action(self, context, action):
        return fl.FlightDescriptor.for_command("Action executed")

    def get_flight_info(self, context, descriptor):
        descriptor_ok = (descriptor.descriptor_type == fl.DescriptorType.PATH) and (
            len(descriptor.path) == 1
        )
        if not descriptor_ok:
            raise fl.FlightServerError(
                f"Descriptor {descriptor} is not supported. Only single-component path descriptors are supported"
            )
        ticket = descriptor.path[0]
        endpoints = [pa.flight.FlightEndpoint(ticket, [self._location])]
        return fl.FlightInfo(self._schema, descriptor, endpoints)


class NoOpAuthHandler(fl.ServerAuthHandler):
    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        return ""


class BasicAuthServerMiddlewareFactory(fl.ServerMiddlewareFactory):
    def __init__(self, creds):
        self.creds = creds

    def start_call(self, info, headers):
        auth_header = None
        for header in headers:
            if header.lower() == "authorization":
                auth_header = headers[header]
                break

        if not auth_header:
            raise fl.FlightUnauthenticatedError("No credentials supplied")

        if not auth_header[0].startswith("Basic ") and not auth_header[0].startswith(
            "Bearer "
        ):
            raise fl.FlightUnauthenticatedError("No credentials supplied")

        token = auth_header[0].split(" ", 1)[1]
        decoded = base64.b64decode(token)
        pair = decoded.decode("utf-8").split(":")
        if pair[0] not in self.creds:
            raise fl.FlightUnauthenticatedError("Unknown user")
        if pair[1] != self.creds[pair[0]]:
            raise fl.FlightUnauthenticatedError("Wrong password")
        return BasicAuthServerMiddleware(token)


class BasicAuthServerMiddleware(fl.ServerMiddleware):
    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        return {"authorization": f"Bearer {self.token}"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", help="Port to serve.", type=int, required=True)
    parser.add_argument("--username", help="Specifies username.", type=str, default="")
    parser.add_argument("--password", help="Specifies password.", type=str, default="")
    args = parser.parse_args()

    location = f"grpc+tcp://0.0.0.0:{args.port}"
    auth_handler = None
    middleware = None
    use_basic_authentication = args.username != ""

    if use_basic_authentication:
        auth_handler = NoOpAuthHandler()
        middleware = {
            "basic": BasicAuthServerMiddlewareFactory({args.username: args.password})
        }

    flight_server = FlightServer(
        location=location, auth_handler=auth_handler, middleware=middleware
    )
    flight_server.serve()
