import pyarrow.flight as fl
import pyarrow as pa
import json

class FlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)
        self._location = location
        self._schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        self._tables = dict()
        self._empty_table = pa.table({'column1': pa.array([]), 'column2': pa.array([])}, schema=self._schema)
        column1_data = pa.array(["test_value_1", "abcadbc", "123456789"])
        column2_data = pa.array(["data1", "text_text_text", "data3"])
        self._tables['ABC'] = pa.table({'column1': column1_data, 'column2': column2_data}, schema=self._schema)

    def do_get(self, context, ticket):
        dataset = ticket.ticket.decode()
        table = self._tables[dataset] if (dataset in self._tables) else self._empty_table
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
        return fl.SchemaResult(self._schema)

    def do_action(self, context, action):
        return fl.FlightDescriptor.for_command("Action executed")

    def get_flight_info(self, context, descriptor):
        path_0 = descriptor.path[0].decode()
        dataset = json.loads(path_0)['dataset']
        endpoints = [pa.flight.FlightEndpoint(dataset, [self._location])]
        return fl.FlightInfo(self._schema, descriptor, endpoints)


if __name__ == "__main__":
    location = "grpc+tcp://0.0.0.0:5005"
    flight_server = FlightServer(location)
    flight_server.serve()
