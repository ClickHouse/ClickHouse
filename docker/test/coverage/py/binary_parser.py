from struct import unpack


class BinaryParser:
    def __init__(self):
        self.is_le = False

    def read_uint32(self, f):
        data = f.read(4)

        if len(data) != 4:
            raise Exception()

        return unpack("<I" if self.is_le else ">I", data)[0]

    def read_str(self, f):
        return f.read(self.read_uint32(f) * 4).rstrip(b"\0").decode("utf-8")
