import pyarrow.flight as fl
import pyarrow as pa

class FlightServer(fl.FlightServerBase):
    def __init__(self, location):
        super().__init__(location)

    def do_get(self, context, ticket):
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        column1_data = pa.array(["test_value_1", "abcadbc", "123456789"])
        column2_data = pa.array(["data1", "text_text_text", "data3"])
        
        table = pa.table({'column1': column1_data, 'column2': column2_data}, schema=schema)
        
        return fl.RecordBatchStream(table)

    def get_schema(self, context, descriptor):
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        return fl.SchemaResult(schema)

    def do_action(self, context, action):
        return fl.FlightDescriptor.for_command("Action executed")

    def do_flight_info(self, context, descriptor):
        schema = pa.schema([('column1', pa.string()), ('column2', pa.string())])
        return fl.FlightInfo(
            schema,
            descriptor,
            1,
            0,
            None,
            None
        )

if __name__ == "__main__":
    location = "grpc+tcp://0.0.0.0:5005"
    flight_server = FlightServer(location)
    flight_server.serve()
