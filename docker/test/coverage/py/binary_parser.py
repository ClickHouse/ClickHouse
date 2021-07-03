from struct import unpack

class BinaryParser:
    LE = "<I"
    BE = ">I"

    def __init__(self):
        self.is_le = False

    @staticmethod
    def read_quad_char(f):
        return f.read(4)

    def read_uint32(self, f):
        data = f.read(4)

        if len(data) != 4:
            raise Exception()

        return unpack(self.LE if self.is_le else self.BE, data)[0]

    def read_string(self, f):
        return f.read(self.read_uint32(f)).decode("utf-8")

    def read_padded_string(self, f):
        word_len = self.read_uint32(f)
        return f.read(word_len * 4).rstrip(b"\0").decode("utf-8")
