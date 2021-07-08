from argparse import ArgumentParser
from jinja2 import Environment, FileSystemLoader, ModuleLoader
import os.path

from gcno_parser import GCNOParser
from ccr_parser import CCRParser
from converter import Converter

parser = ArgumentParser(
    prog='HTML report generator', description="""
Reads .ccr report, generates an HTML coverage report out of it.
Also reads CH source files and corresponding .gcno files.""")

parser.add_argument('report_file', help=".ccr report file")

parser.add_argument(
    'sources_dir', help="Absolute path to ClickHouse sources root directory")

parser.add_argument(
    'gcno_dir', help="Absolute path to directory with .gcno files")

parser.add_argument('out_dir', help="Absolute path to output directory")

args = parser.parse_args()

dir_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..")
tpl_path = os.path.join(dir_path, "templates")
compiled_tpl_path = os.path.join(dir_path, "compiled_templates")

env = Environment(
    loader=FileSystemLoader(tpl_path), trim_blocks=True, enable_async=True)

# comment these 2 lines for readable errors
env.compile_templates(compiled_tpl_path, zip=None, ignore_errors=False)
env.loader = ModuleLoader(compiled_tpl_path)

sf_to_funcs = GCNOParser().read(args.gcno_dir)

tests_names_file = os.path.join(args.sources_dir, "tests", "tests_names.txt")

files, tests, bb = CCRParser().read(args.report_file, tests_names_file)

conv = Converter(sf_to_funcs, files, tests, bb)

for src, func, block in conv.bb_to_src_func_block.values():
    print(src, func, block)
