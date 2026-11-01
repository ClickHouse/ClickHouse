export const EnterprisePlanFeatureBadge = ({feature='이 기능', support=false, linking_verb_are = false}) => {
    return (
        <div className="enterprisePlanFeatureContainer">
            <div className="enterprisePlanFeatureBadge">
                Enterprise 플랜 기능
            </div>
            <div>
                <p>{feature} {linking_verb_are ? '은' : '은'} Enterprise 플랜에서 사용할 수 있습니다. {support ? `이 기능을 활성화하려면 지원팀에 문의하십시오.` : '업그레이드하려면 Cloud Console의 요금제 페이지로 이동하십시오.'}</p>
            </div>
        </div>
    )
}