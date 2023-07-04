WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
if [ ! -d "${WORKING_DIR}/output" ]; then
mkdir ${WORKING_DIR}/output
fi
bash allin1_ssb.sh 2 > ${WORKING_DIR}/output/run.log
echo "Please check log in: ${WORKING_DIR}/output/run.log"