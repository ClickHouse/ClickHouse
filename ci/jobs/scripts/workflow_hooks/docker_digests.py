import sys

from praktika.info import Info

sys.path.append("./tests/ci")
from digest_helper import DockerDigester

# TODO: script ensures seamless migration to praktika
#  it stores docker digests in old format for legacy ci jobs, as a praktika pipeline pre-hook
#  to be removed once migrated

if __name__ == "__main__":
    image_to_digest_map = DockerDigester().get_all_digests()
    Info().store_custom_data(key="digest_dockers", value=image_to_digest_map)
