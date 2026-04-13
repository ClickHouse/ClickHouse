DOCKER_IMAGE=$(echo "$DOCKER_IMAGE" | sed 's/[\/:]/_/g')

if [ "$PR_NUMBER" -eq 0 ]; then
    PREFIX="REFs/$GITHUB_REF_NAME/$COMMIT_SHA/grype/$DOCKER_IMAGE"
else
    PREFIX="PRs/$PR_NUMBER/$COMMIT_SHA/grype/$DOCKER_IMAGE"
fi

S3_PATH="s3://$S3_BUCKET/$PREFIX"
HTTPS_RESULTS_PATH="https://$S3_BUCKET.s3.amazonaws.com/index.html#$PREFIX/"
HTTPS_REPORT_PATH="https://s3.amazonaws.com/$S3_BUCKET/$PREFIX/results.html"
echo "https_report_path=$HTTPS_REPORT_PATH" >> $GITHUB_OUTPUT

tfs --no-colors transform nice raw.log nice.log.txt
tfs --no-colors report results -a $HTTPS_RESULTS_PATH raw.log - --copyright "Altinity LTD" | tfs --no-colors document convert > results.html

aws s3 cp --no-progress nice.log.txt $S3_PATH/nice.log.txt --content-type "text/plain; charset=utf-8" || echo "nice log file not found".
aws s3 cp --no-progress results.html $S3_PATH/results.html || echo "results file not found".
aws s3 cp --no-progress raw.log $S3_PATH/raw.log || echo "raw.log file not found".
aws s3 cp --no-progress result.json $S3_PATH/result.json --content-type "text/plain; charset=utf-8" || echo "result.json not found".