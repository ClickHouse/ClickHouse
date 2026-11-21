"""Check the Rust import observation for a cc_library and its static archive."""

from pathlib import Path
import tempfile
import unittest

from bazel.initial_migration.rust_import_evidence import inspect


class RustImportEvidenceTest(unittest.TestCase):
    def test_cc_library_symbol_manifest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            archive = str(root / "libffi.a")
            magic = b"!<arch>\n"
            Path(archive).write_bytes(magic)
            (root / "magic").write_bytes(magic)
            (root / "status").write_text("0\n")
            (root / "members").write_text(f"{archive}(ffi.o): ffi T 0 1\n")
            (root / "objects").write_text("caller.o: ffi U 0 0\n")
            caller, imported = "//:caller", "//:ffi"
            data = {
                "owner": {"target": caller, "records": [0]},
                "deps": [{"target": imported, "records": [1]}], "implementation_deps": [],
                "inventory": [
                    {"owner": caller, "libraries": [{"objects": ["caller.o"]}]},
                    {"owner": imported, "flags": [], "inputs": [], "linkstamps": [], "libraries": [{
                        "objects": [], "object_owners": {}, "unchecked_objects": [], "static": archive,
                        "pic_static": "", "dynamic": "", "interface": "", "alwayslink": False,
                    }]},
                ],
                "objects": ["caller.o"], "symbols": str(root / "objects"),
                "archives": {archive: {k: str(root / f) for k, f in
                                      (("symbols", "members"), ("status", "status"), ("magic", "magic"))}},
                "executable_objects": [], "incomplete_executable_objects": False,
            }
            self.assertEqual(inspect(data, caller, imported)["object_matches"],
                             {"caller.o": {"symbol": "ffi", "member": "ffi.o"}})
            for field in ("executable_objects", "incomplete_executable_objects"):
                with self.subTest(missing=field), self.assertRaises(ValueError):
                    inspect({k: v for k, v in data.items() if k != field}, caller, imported)
            for fields in ({"unknown": []}, {"executable_objects": ["main.o"]},
                           {"incomplete_executable_objects": True}, {"incomplete_executable_objects": 0}):
                with self.subTest(changed=fields), self.assertRaises(ValueError):
                    inspect({**data, **fields}, caller, imported)


if __name__ == "__main__":
    unittest.main()
